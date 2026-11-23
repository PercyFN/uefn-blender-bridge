"""UEFN Blender Bridge — UEFN Side.

Architecture:
  - HTTP server receives commands from Blender addon
  - FBX import via AssetImportTask + FbxImportUI
  - PBR material creation via MaterialEditingLibrary
  - Tkinter dashboard for status and configuration
  - Tick-integrated command execution (thread-safe)

Run: UEFN > Tools > Execute Python Script > this file

github.com/KiKoZl1 | Surprise Co. (surpriseugc.com)
"""

import unreal
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import queue
import re
import threading
import time
import traceback
import urllib.request
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any, Dict, List, Optional

# ============================================================
# CONFIG
# ============================================================

PLUGIN_VERSION = "1.0.2"
DEFAULT_PORT = 8790
MAX_PORT = 8795
TICK_BATCH = 5
MAX_REQUEST_BYTES = 8 * 1024 * 1024
HTTP_TIMEOUT = 600.0   # UEFN-side do_POST deadline; a heavy textured import can take minutes and
                       # the UEFN import pipeline is single-threaded, so a short deadline 504'd mid-import
                       # (Blender already waits 600s on the send). Must be >= the Blender send timeout.
POLL_INTERVAL = 0.02
STALE_SEC = 120.0
ACTOR_PREFIX = "BB_"
LIVE_POLL_INTERVAL = 0.4   # seconds between UEFN->Blender transform polls when Live Sync is on
# Commands that write BB_ actor transforms from Blender data — after one runs, the push
# baseline is refreshed so the live poll doesn't echo Blender's own writes straight back.
INBOUND_WRITE_CMDS = frozenset(
    ("import_scene", "import_baked", "add_objects", "update_objects", "update_transforms"))

# ============================================================
# GLOBALS
# ============================================================

_http_server = None
_http_thread = None
_bound_port = 0
_command_queue = queue.Queue()
_responses = {}
_responses_lock = threading.Lock()
_req_counter = 0

_project_path = ""
_world_path = ""
_bridge_project = ""       # project name from Blender (folder under BlenderBridge/)
_blender_server_port = 0   # Blender's HTTP server port for bidirectional sync
_last_push_snapshot = {}   # {obj_name: "loc|rot|scale" hash} for diff-based push
_live_sync_active = False  # True while Blender Live Sync is on -> UEFN polls + pushes changes
_eal_cache = None          # cached EditorAssetSubsystem (modern) or EditorAssetLibrary fallback
_import_scale = 1.0
_auto_collision = True

_blender_info = {}
_imported_scenes = {}
_last_import = 0.0
_total_imports = 0
_log_ring = []
_gui = None
_session_dir = None
_exchange_dir = None
_session_token = None


def _load_session():
    global _session_dir, _exchange_dir, _session_token
    configured = os.environ.get("UEFN_BLENDER_BRIDGE_SESSION_DIR")
    if not configured:
        raise RuntimeError("Set UEFN_BLENDER_BRIDGE_SESSION_DIR before starting the bridge.")
    _session_dir = Path(configured).resolve(strict=True)
    if not _session_dir.is_dir():
        raise RuntimeError("Bridge session path must be a directory.")
    _exchange_dir = (_session_dir / "exchange").resolve(strict=True)
    token_path = (_session_dir / "session_token").resolve(strict=True)
    if (_exchange_dir.parent != _session_dir or not _exchange_dir.is_dir()
            or token_path.parent != _session_dir or not token_path.is_file()):
        raise RuntimeError("Bridge exchange and session token must be inside the session directory.")
    if token_path.stat().st_size > 128:
        raise RuntimeError("Invalid bridge session token.")
    _session_token = token_path.read_text(encoding="utf-8-sig").strip()
    if not re.fullmatch(r"[0-9a-fA-F]{64}", _session_token):
        raise RuntimeError("Bridge session token must contain 64 hex characters.")


def _exchange_path(value, suffixes=(), directory=False):
    if not isinstance(value, (str, os.PathLike)) or not value:
        raise ValueError("A bridge exchange path is required.")
    raw = Path(value)
    if not raw.is_absolute():
        raise ValueError("Bridge exchange paths must be absolute.")
    path = raw.resolve(strict=True)
    if not path.is_relative_to(_exchange_dir):
        raise ValueError("Bridge paths must stay inside the session exchange directory.")
    if directory:
        if not path.is_dir():
            raise ValueError("Bridge exchange folder is not a directory.")
    elif not path.is_file() or path.suffix.lower() not in suffixes:
        raise ValueError("Unexpected file type in the bridge exchange directory.")
    return str(path)


_TEXTURE_SUFFIXES = (".png", ".jpg", ".jpeg", ".tga", ".bmp", ".tiff", ".exr")


def _read_material_manifest(exchange_folder):
    folder = _exchange_path(exchange_folder, directory=True)
    manifest = Path(folder) / "materials.json"
    if not manifest.exists():
        return []
    path = Path(_exchange_path(manifest, (".json",)))
    if path.stat().st_size > MAX_REQUEST_BYTES:
        raise ValueError("Bridge material manifest is too large.")
    with path.open("r", encoding="utf-8-sig") as handle:
        entries = json.load(handle)
    if not isinstance(entries, list):
        raise ValueError("Bridge material manifest must contain a list.")
    for entry in entries:
        for texture in (entry.get("textures") or {}).values():
            if texture.get("path"):
                texture["path"] = _exchange_path(texture["path"], _TEXTURE_SUFFIXES)
    return entries


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_loopback_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())

# ============================================================
# CHANNEL MAP
# ============================================================

CHANNEL_MAP = {
    "basecolor": "BaseColor", "base_color": "BaseColor", "diffuse": "BaseColor",
    "albedo": "BaseColor", "color": "BaseColor",
    "normal": "Normal", "normalmap": "Normal", "normal_map": "Normal",
    "roughness": "Roughness", "metallic": "Metallic", "metalness": "Metallic",
    "height": "Height", "displacement": "Height",
    "ambientocclusion": "AO", "ambient_occlusion": "AO", "ao": "AO",
    "emissive": "Emissive", "emission": "Emissive",
    "opacity": "Opacity", "specular": "Specular",
}

MAT_PROP = {
    "BaseColor": "MP_BASE_COLOR", "Normal": "MP_NORMAL",
    "Roughness": "MP_ROUGHNESS", "Metallic": "MP_METALLIC",
    "Emissive": "MP_EMISSIVE_COLOR", "Opacity": "MP_OPACITY",
    "Specular": "MP_SPECULAR", "AO": "MP_AMBIENT_OCCLUSION",
}

CH_DISPLAY = {
    "BaseColor": "Base Color", "Normal": "Normal", "Roughness": "Roughness",
    "Metallic": "Metallic", "Height": "Height", "AO": "AO",
    "Emissive": "Emissive", "Opacity": "Opacity", "Specular": "Specular",
}

CH_COLORS = {
    "BaseColor": "#e07840", "Normal": "#7b7de0", "Roughness": "#5a9a5a",
    "Metallic": "#a8a8a8", "Height": "#8a7a5e", "AO": "#606060",
    "Emissive": "#d0d040", "Opacity": "#4090d0", "Specular": "#d04040",
}

# ============================================================
# HELPERS
# ============================================================

def _log(msg, level="info"):
    ts = time.strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    _log_ring.append(entry)
    if len(_log_ring) > 300:
        _log_ring.pop(0)
    tag = "[BlenderBridge]"
    if level == "error":
        unreal.log_error(f"{tag} {msg}")
    elif level == "warning":
        unreal.log_warning(f"{tag} {msg}")
    else:
        unreal.log(f"{tag} {msg}")
    if _gui:
        _gui._on_log(entry, level)


def _base_dir(name=""):
    """Root directory for current bridge project."""
    base = _project_path.strip("/") if _project_path else "Game"
    pname = _bridge_project or "Default"
    root = f"/{base}/BlenderBridge/{pname}"
    return f"{root}/{name}" if name else root


def _mesh_dir(scene_name=""):
    """Directory for mesh assets."""
    root = _base_dir("Meshes")
    return f"{root}/{scene_name}" if scene_name else root


def _mat_dir(scene_name=""):
    """Directory for material assets."""
    root = _base_dir("Materials")
    return f"{root}/{scene_name}" if scene_name else root


def _tex_dir(scene_name=""):
    """Directory for texture assets."""
    root = _base_dir("Textures")
    return f"{root}/{scene_name}" if scene_name else root


def _sanitize_seg(s):
    """UEFN-safe asset/path-segment name. UEFN's content validator is STRICTER than vanilla UE:
    UE's INVALID_OBJECTNAME_CHARACTERS does NOT list '-', but UEFN rejects it ("Name cannot
    contain the following characters: -"), and a leading digit is also rejected. So instead of a
    fragile blacklist we WHITELIST ASCII [A-Za-z0-9_] (the safe intersection, per the Allar UE5
    Style Guide) — every other char (space, '-', '.', ':', punctuation, non-ASCII) becomes '_' —
    and never start with a digit. Empty -> 'Unnamed'."""
    out = "".join(ch if (ch.isascii() and ch.isalnum()) else "_" for ch in (s or ""))
    if not out:
        return "Unnamed"
    if out[0].isdigit():
        out = "_" + out
    return out


def _collection_folder(root, collection):
    """Asset folder for a Blender collection under a type root (F-16). Mirrors the Blender
    collection hierarchy (no scene wrapper); empty collection -> the type root. Each segment is
    sanitized for the Content Browser (the Outliner folder keeps the original name with spaces)."""
    c = (collection or "").strip("/")
    if not c:
        return root
    segs = [_sanitize_seg(s) for s in c.split("/") if s]
    segs = [s for s in segs if s]
    return root + "/" + "/".join(segs) if segs else root


def _outliner_folder(collection=""):
    """World Outliner folder for bridge actors: /BlenderBridge/<project>/<collection> — mirrors
    the Content Browser hierarchy. Collection sits loose under the project root when empty."""
    base = f"/BlenderBridge/{_bridge_project}" if _bridge_project else "/BlenderBridge"
    c = (collection or "").strip("/")
    return f"{base}/{c}" if c else base


def _asset_lib():
    """Asset ops via EditorAssetSubsystem (modern) when available, else the deprecated-but-
    working EditorAssetLibrary. Both expose the same method names (does_asset_exist, load_asset,
    save_asset, delete_asset, list_assets, rename_asset, find_asset_data, does/delete_directory),
    so call sites are identical. Safe on restrictive builds — falls back instead of breaking."""
    global _eal_cache
    if _eal_cache is None:
        try:
            _eal_cache = unreal.get_editor_subsystem(unreal.EditorAssetSubsystem)
        except Exception:
            _eal_cache = None
        if _eal_cache is None:
            _eal_cache = unreal.EditorAssetLibrary
    return _eal_cache


def _detect_project_path():
    global _project_path, _world_path
    _world_path = _current_world_path()
    if not _world_path.startswith("/") or _world_path.count("/") < 2:
        raise RuntimeError("Open the target project and level before starting the bridge.")
    _project_path = "/" + _world_path.split("/")[1]
    _log(f"Project path: {_project_path}")


def _current_world_path():
    world = unreal.get_editor_subsystem(unreal.UnrealEditorSubsystem).get_editor_world()
    return world.get_path_name() if world else ""


def _check_world():
    if not _world_path or _current_world_path() != _world_path:
        raise RuntimeError("The original bridge level is no longer open. Restart the bridge for this level.")


def _detect_channel(filename):
    n = os.path.splitext(filename)[0].lower()
    for key, ch in CHANNEL_MAP.items():
        if n.endswith(key) or n.endswith("_" + key):
            return ch
    return None


def _get_set_name(filename):
    n = os.path.splitext(filename)[0]
    nl = n.lower()
    for key in CHANNEL_MAP:
        if nl.endswith("_" + key):
            return n[:-(len(key) + 1)]
        if nl.endswith(key):
            return n[:-len(key)].rstrip("_")
    return n


def _serialize(obj):
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, (list, tuple)):
        return [_serialize(v) for v in obj]
    if isinstance(obj, dict):
        return {str(k): _serialize(v) for k, v in obj.items()}
    if hasattr(obj, "get_path_name"):
        return str(obj.get_path_name())
    try:
        return str(obj)
    except Exception:
        return repr(obj)


def _asset_class(path):
    try:
        ad = _asset_lib().find_asset_data(path)
        return str(ad.asset_class_path.asset_name) if hasattr(ad, "asset_class_path") else str(getattr(ad, "asset_class", ""))
    except Exception:
        return ""

# ============================================================
# TEXTURE IMPORT
# ============================================================

def _import_tex(fp, dest, name):
    task = unreal.AssetImportTask()
    task.set_editor_property("filename", fp)
    task.set_editor_property("destination_path", dest)
    task.set_editor_property("destination_name", name)
    task.set_editor_property("replace_existing", True)
    task.set_editor_property("automated", True)
    # Don't save here — _config_tex sets the right compression/sRGB and saves once. Saving on
    # import too would write each texture to disk twice (the wrong compression, then the right one).
    task.set_editor_property("save", False)
    unreal.AssetToolsHelpers.get_asset_tools().import_asset_tasks([task])
    exp = f"{dest}/{name}"
    if _asset_lib().does_asset_exist(exp):
        return exp
    try:
        ps = task.get_editor_property("imported_object_paths")
        if ps and len(ps) > 0:
            return str(ps[0])
    except Exception:
        pass
    return None


def _config_tex(path, ch):
    tex = _asset_lib().load_asset(path)
    if not tex:
        return
    try:
        if ch == "Normal":
            tex.set_editor_property("compression_settings", unreal.TextureCompressionSettings.TC_NORMALMAP)
            tex.set_editor_property("srgb", False)
        elif ch in ("Roughness", "Metallic", "AO", "Height", "Opacity", "Specular"):
            tex.set_editor_property("compression_settings", unreal.TextureCompressionSettings.TC_MASKS)
            tex.set_editor_property("srgb", False)
        elif ch in ("BaseColor", "Emissive"):
            tex.set_editor_property("srgb", True)
        _asset_lib().save_asset(path)
    except Exception:
        pass

# ============================================================
# FBX IMPORT
# ============================================================

def _import_fbx(fbx_path, dest_path, combine=False, existing_name=None):
    if not os.path.exists(fbx_path):
        _log(f"FBX not found: {fbx_path}", "error")
        return []

    _log(f"Importing FBX: {os.path.basename(fbx_path)} -> {dest_path}")

    task = unreal.AssetImportTask()
    task.set_editor_property("filename", fbx_path)
    task.set_editor_property("destination_path", dest_path)
    task.set_editor_property("destination_name", existing_name or "")
    task.set_editor_property("replace_existing", True)
    task.set_editor_property("replace_existing_settings", False)
    task.set_editor_property("automated", True)
    task.set_editor_property("save", existing_name is None)
    if existing_name:
        # In-place update needs a factory that respects destination_name.
        task.set_editor_property("factory", unreal.FbxFactory())

    options = unreal.FbxImportUI()
    options.set_editor_property("import_mesh", True)
    # Embedded materials/textures OFF: our MM_/MI_ pipeline rebuilds and assigns every material
    # from the separate texture export (it was overwriting the embed ones anyway), so importing
    # them was pure waste — dozens of orphan assets that then cost ~3 min to delete from staging.
    # Mesh sections (material slots) still come from geometry, so per-slot MI_ assignment is intact.
    options.set_editor_property("import_textures", False)
    options.set_editor_property("import_materials", False)
    options.set_editor_property("import_as_skeletal", False)
    options.set_editor_property("import_animations", False)
    options.set_editor_property("automated_import_should_detect_type", False)
    options.set_editor_property("mesh_type_to_import", unreal.FBXImportType.FBXIT_STATIC_MESH)

    sm = options.get_editor_property("static_mesh_import_data")
    sm.set_editor_property("combine_meshes", combine)
    sm.set_editor_property("auto_generate_collision", _auto_collision)
    # No conversions on import — the FBX is already baked to cm + Z-up on export, so this
    # works whether UEFN honors these flags (legacy FBX) or ignores them (Interchange).
    sm.set_editor_property("convert_scene", False)
    sm.set_editor_property("convert_scene_unit", False)

    task.set_editor_property("options", options)
    unreal.AssetToolsHelpers.get_asset_tools().import_asset_tasks([task])

    # Collect imported assets
    imported = []
    try:
        paths = task.get_editor_property("imported_object_paths")
        if paths:
            imported = [str(p) for p in paths]
    except Exception:
        pass

    if not imported and not existing_name:
        try:
            reg = unreal.AssetRegistryHelpers.get_asset_registry()
            for ad in reg.get_assets_by_path(dest_path, recursive=True):
                cls = str(ad.asset_class_path.asset_name) if hasattr(ad, "asset_class_path") else ""
                if "StaticMesh" in cls:
                    imported.append(str(ad.get_asset().get_path_name()))
        except Exception:
            pass

    _log(f"Imported {len(imported)} asset(s)")
    return imported

# ============================================================
# MESH PLACEMENT
# ============================================================



def _actor_guid(actor):
    if not _bridge_project:
        return None
    tags = [str(tag) for tag in actor.tags]
    project_tags = [tag for tag in tags if tag.startswith("BB_PROJECT:")]
    if project_tags != [f"BB_PROJECT:{_bridge_project}"]:
        return None
    guids = [tag.removeprefix("BB_GUID:") for tag in tags if tag.startswith("BB_GUID:")]
    if len(guids) != 1 or not re.fullmatch(r"[0-9a-f]{32}", guids[0]):
        return None
    return guids[0]


def _owned_actors(asub):
    by_guid = {}
    for actor in asub.get_all_level_actors():
        guid = _actor_guid(actor)
        if guid:
            if guid in by_guid:
                raise ValueError("Duplicate actor GUID in the current bridge project.")
            by_guid[guid] = actor
    return by_guid


def _validated_objects(objects):
    if not _bridge_project or not isinstance(objects, list) or not objects:
        raise ValueError("Connect from Blender and provide mesh objects with stable GUIDs.")
    seen = set()
    for entry in objects:
        if not isinstance(entry, dict) or not isinstance(entry.get("name"), str) or not entry["name"]:
            raise ValueError("Every bridge mesh object must have a name.")
        guid = entry.get("guid", "")
        if not isinstance(guid, str) or not re.fullmatch(r"[0-9a-f]{32}", guid) or guid in seen:
            raise ValueError("Bridge objects must have unique 32-character lowercase GUIDs.")
        for field in ("location", "rotation", "scale"):
            values = entry.get(field)
            if (not isinstance(values, list) or len(values) != 3
                    or not all(type(value) in (int, float) and math.isfinite(value) for value in values)):
                raise ValueError(f"Bridge object {entry['name']} has invalid {field}.")
        seen.add(guid)
    return objects


def _find_actor_by_guid(asub, guid):
    return _owned_actors(asub).get(guid)


def _ensure_sm_name(asset, base, dest_folder=None):
    """Rename/move an imported StaticMesh to <dest_folder>/SM_<base> (F-43 naming + F-16
    collection subfolder). dest_folder defaults to the asset's current folder. Reuses an
    existing asset at the destination (mesh dedup — F-44). Falls back safely."""
    try:
        pkg = asset.get_path_name().split(".")[0]
        cur_folder, cur = pkg.rsplit("/", 1)
        folder = dest_folder or cur_folder
        new_pkg = f"{folder}/SM_{base}"
        if new_pkg == pkg:
            return asset
        # An imported mesh with a conflicting final path requires a targeted reimport.
        if _asset_lib().does_asset_exist(new_pkg):
            raise ValueError(f"Bridge mesh already exists at {new_pkg}.")
        ok = False
        try:
            ok = _asset_lib().rename_asset(pkg, new_pkg)
        except Exception as e:
            _log(f"  SM_ rename error {cur} -> {new_pkg}: {e}", "warning")
        if ok:
            moved = _asset_lib().load_asset(new_pkg)
            if moved:
                _log(f"  named SM_{base} @ {folder}")
                return moved
        raise RuntimeError(f"Cannot move bridge mesh to {new_pkg}.")
    except Exception as e:
        raise RuntimeError(f"Cannot route bridge mesh {base}: {e}") from e


def _spawn_or_reuse(asub, asset, label, guid):
    """Match only bridge-owned actor IDs and preserve existing editor tags."""
    if not _bridge_project or not re.fullmatch(r"[0-9a-f]{32}", guid):
        raise ValueError("Bridge actor requires project ownership and a unique GUID.")
    existing = _find_actor_by_guid(asub, guid)
    if existing:
        comp = existing.static_mesh_component
        if not comp:
            raise ValueError("Bridge actor does not have a static mesh component.")
        if comp.get_editor_property("static_mesh") != asset:
            comp.set_static_mesh(asset)
        return existing
    actor = asub.spawn_actor_from_object(asset, unreal.Vector(0, 0, 0))
    if not actor:
        _log(f"  Spawn failed: {label}", "warning")
        return None
    actor.set_actor_label(label)
    actor.tags = list(actor.tags) + [
        unreal.Name(f"BB_GUID:{guid}"),
        unreal.Name(f"BB_PROJECT:{_bridge_project}"),
    ]
    return actor


def _apply_obj_transform(actor, od):
    """Apply the per-object WORLD transform Blender sent (already in UE coords) + Outliner folder.
    Placement mirrors the Blender position — never a camera offset."""
    loc = od.get("location", [0, 0, 0])
    rot = od.get("rotation", [0, 0, 0])   # [pitch, yaw, roll]
    scale = od.get("scale", [1, 1, 1])
    collection = od.get("collection", "")
    actor.set_actor_location(unreal.Vector(loc[0], loc[1], loc[2]), False, False)
    actor.set_actor_rotation(unreal.Rotator(pitch=rot[0], yaw=rot[1], roll=rot[2]), False)
    actor.set_actor_scale3d(unreal.Vector(scale[0], scale[1], scale[2]))
    actor.set_folder_path(_outliner_folder(collection))


def _enable_nanite(sm):
    """Enable Nanite on an imported StaticMesh — better UEFN runtime perf and it helps stay under
    the 100k memory-unit publish gate. Defensive: any API difference on a given build just logs and
    is ignored, so the mesh always imports even if Nanite can't be set. Applied once per unique
    mesh (instanced actors share the asset)."""
    if not sm:
        return
    try:
        ns = sm.get_editor_property("nanite_settings")
        ns.set_editor_property("enabled", True)
        sm.set_editor_property("nanite_settings", ns)
        _asset_lib().save_asset(sm.get_path_name().split(".")[0])
    except Exception as e:
        _log(f"  Nanite enable skipped: {e}", "warning")


def _place_meshes(asset_paths, object_data=None, scene_name="Scene"):
    """Place actors. OBJECT-DRIVEN: each object references a (possibly shared) StaticMesh via its
    'mesh' field — N Blender objects sharing one mesh datablock spawn N actors off ONE imported
    mesh (instancing — F-44). Falls back to one-actor-per-mesh when no object data is given."""
    asub = unreal.get_editor_subsystem(unreal.EditorActorSubsystem)
    actors = []

    # Load imported StaticMeshes, keyed by FBX-style source name (dots->underscores).
    mesh_by_name = {}
    for path in asset_paths:
        try:
            asset = unreal.load_asset(str(path))
            if not asset:
                clean = str(path).split(" ")[-1] if " " in str(path) else str(path)
                asset = unreal.load_asset(clean)
            if not asset or not isinstance(asset, unreal.StaticMesh):
                continue
            mesh_by_name[str(path).split("/")[-1].split(".")[0]] = asset
        except Exception as e:
            _log(f"  Load error: {path}: {e}", "warning")

    if not mesh_by_name:
        _log("  No StaticMeshes to place", "warning")
        return actors

    def _loose(s):   # alphanumeric-only, lowercased — tolerant of any char UE swapped to '_'
        return "".join(c for c in (s or "").lower() if c.isalnum())

    def _resolve(key):
        # UE sanitizes asset names (spaces/dots -> '_'), so sanitize the query the SAME way —
        # the old code only handled dots, so a mesh with a SPACE in its name (e.g. "Human
        # Architecture Building" -> asset "Human_Architecture_Building") never matched and the
        # object was silently skipped. Loose alphanumeric compare is the final fallback.
        k = _sanitize_seg(key or "")
        if k in mesh_by_name:
            return k, mesh_by_name[k]
        lk = _loose(key)
        for mk, asset in mesh_by_name.items():
            if _loose(mk) == lk:
                return mk, asset
        if len(mesh_by_name) == 1:   # combined/single-mesh export fallback
            return next(iter(mesh_by_name.items()))
        return None, None

    renamed = {}  # mesh key -> StaticMesh after SM_/collection rename (once per shared mesh)
    for od in object_data:
        try:
            bname = od.get("name", "")
            guid = od.get("guid", "")
            collection = od.get("collection", "")
            mesh_key = od.get("mesh", bname)   # representative name (instancing)
            label = f"{ACTOR_PREFIX}{bname}"

            rk, asset = _resolve(mesh_key)
            if not asset:
                _log(f"  No mesh '{mesh_key}' for object '{bname}'", "warning")
                continue

            # Name/move the shared StaticMesh ONCE: <Meshes>/<collection>/SM_<rep> (F-16/F-43).
            if rk in renamed:
                asset = renamed[rk]
            else:
                asset = _ensure_sm_name(asset, _sanitize_seg(mesh_key),
                                        _collection_folder(_mesh_dir(), collection))
                _enable_nanite(asset)
                renamed[rk] = asset
                mesh_by_name[rk] = asset

            actor = _spawn_or_reuse(asub, asset, label, guid)
            if not actor:
                continue
            _apply_obj_transform(actor, od)
            actors.append(actor)
            _log(f"Placed: {label} (mesh: {mesh_key})")
        except Exception as e:
            _log(f"Place error: {e}", "warning")

    _log(f"Placed {len(actors)} actor(s)")
    return actors

# ============================================================
# CLEANUP
# ============================================================

# ============================================================
# PBR MATERIAL CREATION
# ============================================================

# Short per-channel codes for shared-master naming (MM_Tex_BC_R, MM_Tex_BC_M_R_S, ...).
_CH_CODE = {
    "BaseColor": "BC", "Normal": "N", "Roughness": "R", "Metallic": "M",
    "Specular": "S", "Emissive": "E", "AO": "AO", "Opacity": "O", "Height": "H",
}
_CH_ORDER = ["BaseColor", "Normal", "Roughness", "Metallic", "Specular",
             "Emissive", "AO", "Opacity", "Height"]


def _tex_master_key(channels):
    """Stable name suffix for the set of channels a textured material uses."""
    return "_".join(_CH_CODE[c] for c in _CH_ORDER if c in channels and c in _CH_CODE)


def _ensure_tex_master(uchan, mel, cache):
    """ONE shared textured master per DISTINCT channel-set, compiled once and reused by every
    material with that exact set. This replaces the old per-material `M_<name>` master, which
    recompiled a shader each time — the ~2-min bottleneck on a real scene. The master's params
    are channel-generic (T_BaseColor, T_Roughness, ...) so any matching MI_ plugs straight in, and
    because every MI on a given master overrides EVERY param the master has, there's no
    default-texture problem. Masters live in the Materials root (shared across collections) and
    persist across imports (asset-exists dedup)."""
    cset = frozenset(uchan.keys())
    if cset in cache:
        return cache[cset]
    root = _base_dir("Materials")
    name = f"MM_Tex_{_tex_master_key(cset)}" if _tex_master_key(cset) else "MM_Tex"
    mp = f"{root}/{name}"
    if not _asset_lib().does_asset_exist(mp):
        mp = _create_parent_material(name, root, uchan, mel) or mp
    cache[cset] = mp
    return mp


def _create_parent_material(mat_name, dest, textures, mel):
    tools = unreal.AssetToolsHelpers.get_asset_tools()
    mat = tools.create_asset(mat_name, dest, unreal.Material, unreal.MaterialFactoryNew())
    if not mat:
        return None
    mp = mat.get_path_name()

    has = lambda ch: ch in textures

    def tex_p(name, x, y, ch):
        tex = _asset_lib().load_asset(textures[ch])
        n = mel.create_material_expression(mat, unreal.MaterialExpressionTextureSampleParameter2D, x, y)
        n.set_editor_property("parameter_name", name)
        n.set_editor_property("texture", tex)
        return n

    def scalar_p(name, val, x, y):
        n = mel.create_material_expression(mat, unreal.MaterialExpressionScalarParameter, x, y)
        n.set_editor_property("parameter_name", name)
        n.set_editor_property("default_value", val)
        return n

    def mul_n(x, y):
        return mel.create_material_expression(mat, unreal.MaterialExpressionMultiply, x, y)

    def lrp_n(x, y):
        return mel.create_material_expression(mat, unreal.MaterialExpressionLinearInterpolate, x, y)

    y_pos = 0

    if has("BaseColor"):
        t = tex_p("T_BaseColor", -800, y_pos, "BaseColor")
        mel.connect_material_property(t, "RGB", unreal.MaterialProperty.MP_BASE_COLOR)
        y_pos += 250

    if has("Normal"):
        t = tex_p("T_Normal", -800, y_pos, "Normal")
        mel.connect_material_property(t, "RGB", unreal.MaterialProperty.MP_NORMAL)
        y_pos += 250

    if has("Roughness"):
        t = tex_p("T_Roughness", -800, y_pos, "Roughness")
        p_min = scalar_p("RoughnessMin", 0.0, -600, y_pos + 50)
        p_max = scalar_p("RoughnessMax", 1.0, -600, y_pos + 100)
        l = lrp_n(-400, y_pos)
        mel.connect_material_expressions(p_min, "", l, "A")
        mel.connect_material_expressions(p_max, "", l, "B")
        mel.connect_material_expressions(t, "R", l, "Alpha")
        mel.connect_material_property(l, "", unreal.MaterialProperty.MP_ROUGHNESS)
        y_pos += 250

    if has("Metallic"):
        t = tex_p("T_Metallic", -800, y_pos, "Metallic")
        mel.connect_material_property(t, "R", unreal.MaterialProperty.MP_METALLIC)
        y_pos += 250

    if has("Emissive"):
        t = tex_p("T_Emissive", -800, y_pos, "Emissive")
        p_int = scalar_p("EmissiveIntensity", 1.0, -600, y_pos + 50)
        m = mul_n(-400, y_pos)
        mel.connect_material_expressions(t, "RGB", m, "A")
        mel.connect_material_expressions(p_int, "", m, "B")
        mel.connect_material_property(m, "", unreal.MaterialProperty.MP_EMISSIVE_COLOR)
        y_pos += 250

    if has("AO"):
        t = tex_p("T_AO", -800, y_pos, "AO")
        mel.connect_material_property(t, "R", unreal.MaterialProperty.MP_AMBIENT_OCCLUSION)
        y_pos += 250

    if has("Opacity"):
        t = tex_p("T_Opacity", -800, y_pos, "Opacity")
        mel.connect_material_property(t, "R", unreal.MaterialProperty.MP_OPACITY)
        y_pos += 250

    if has("Specular"):
        t = tex_p("T_Specular", -800, y_pos, "Specular")
        mel.connect_material_property(t, "R", unreal.MaterialProperty.MP_SPECULAR)
        y_pos += 250

    if has("Height"):
        tex_p("T_Height", -800, y_pos, "Height")
        y_pos += 250

    try:
        mel.recompile_material(mat)
    except Exception:
        pass
    _asset_lib().save_asset(mp)
    _log(f"  Parent material: {mat_name} ({len(textures)} ch)")
    return mp


def _create_mi(mi_name, dest, parent_path, textures):
    mi_path = f"{dest}/{mi_name}"
    if _asset_lib().does_asset_exist(mi_path):
        mi = _asset_lib().load_asset(mi_path)
        if mi:
            _set_mi_tex(mi, mi_path, textures)
            return mi_path
        _asset_lib().delete_asset(mi_path)

    tools = unreal.AssetToolsHelpers.get_asset_tools()
    try:
        factory = unreal.MaterialInstanceConstantFactoryNew()
        mi = tools.create_asset(mi_name, dest, unreal.MaterialInstanceConstant, factory)
    except Exception:
        _log("  MI creation failed, using parent", "warning")
        return parent_path

    if not mi:
        return parent_path

    parent = _asset_lib().load_asset(parent_path)
    if parent:
        mi.set_editor_property("parent", parent)

    mi_path = mi.get_path_name()
    _set_mi_tex(mi, mi_path, textures)
    return mi_path


def _set_mi_tex(mi, mi_path, textures):
    mel = unreal.MaterialEditingLibrary
    pmap = {
        "BaseColor": "T_BaseColor", "Normal": "T_Normal", "Roughness": "T_Roughness",
        "Metallic": "T_Metallic", "AO": "T_AO", "Emissive": "T_Emissive",
        "Height": "T_Height", "Opacity": "T_Opacity", "Specular": "T_Specular",
    }
    for ch, tp in textures.items():
        p = pmap.get(ch)
        if not p:
            continue
        tex = _asset_lib().load_asset(tp)
        if not tex:
            continue
        try:
            mel.set_material_instance_texture_parameter_value(mi, p, tex)
        except Exception:
            pass
    _asset_lib().save_asset(mi_path)


def _apply_material_to_actors(actors, mi_path):
    mat = _asset_lib().load_asset(mi_path)
    if not mat:
        return
    count = 0
    for actor in actors:
        try:
            comp = actor.static_mesh_component
            if comp:
                for i in range(comp.get_num_materials()):
                    comp.set_material(i, mat)
                count += 1
        except Exception:
            pass
    if count:
        _log(f"  Applied material to {count} actor(s)")


def _socket_to_channel(socket):
    """Fallback channel from a Blender Principled input socket (used only when the texture
    filename has no channel suffix). Filename detection (_detect_channel) is primary."""
    return {
        "base color": "BaseColor", "color": "BaseColor", "normal": "Normal",
        "roughness": "Roughness", "metallic": "Metallic", "specular": "Specular",
        "specular ior level": "Specular", "emission": "Emissive",
        "emission color": "Emissive", "alpha": "Opacity",
    }.get((socket or "").lower())


COLOR_MASTER = "MM_BlenderBridge"


def _ensure_color_master():
    """ONE shared master for color-only materials: VectorParameter BaseColor + Scalars
    Metallic/Roughness/Specular. No textures => no default-texture problem, so a single shared
    master works. Created once in Materials root, reused by all color MI_."""
    mat_root = _base_dir("Materials")
    mp = f"{mat_root}/{COLOR_MASTER}"
    if _asset_lib().does_asset_exist(mp):
        return mp
    tools = unreal.AssetToolsHelpers.get_asset_tools()
    mat = tools.create_asset(COLOR_MASTER, mat_root, unreal.Material, unreal.MaterialFactoryNew())
    if not mat:
        return None
    mel = unreal.MaterialEditingLibrary
    try:
        bc = mel.create_material_expression(mat, unreal.MaterialExpressionVectorParameter, -400, 0)
        bc.set_editor_property("parameter_name", "BaseColor")
        bc.set_editor_property("default_value", unreal.LinearColor(0.5, 0.5, 0.5, 1.0))
        mel.connect_material_property(bc, "", unreal.MaterialProperty.MP_BASE_COLOR)
        for i, (pn, prop, dv) in enumerate((
            ("Metallic", unreal.MaterialProperty.MP_METALLIC, 0.0),
            ("Roughness", unreal.MaterialProperty.MP_ROUGHNESS, 0.5),
            ("Specular", unreal.MaterialProperty.MP_SPECULAR, 0.5),
        )):
            s = mel.create_material_expression(
                mat, unreal.MaterialExpressionScalarParameter, -400, 200 + i * 150)
            s.set_editor_property("parameter_name", pn)
            s.set_editor_property("default_value", dv)
            mel.connect_material_property(s, "", prop)
        mel.recompile_material(mat)
    except Exception as e:
        _log(f"  color master build error: {e}", "warning")
    _asset_lib().save_asset(mp)
    _log(f"  Master: {COLOR_MASTER}")
    return mp


def _color_mi(mname, safe, mat_dest, props):
    """Color-only MI_ (instance of the shared MM_BlenderBridge) — overrides BaseColor + scalars
    from the Blender material values. Deduped by path."""
    master = _ensure_color_master()
    if not master:
        return None
    mi_path = f"{mat_dest}/MI_{safe}"
    if _asset_lib().does_asset_exist(mi_path):
        mi = _asset_lib().load_asset(mi_path)
    else:
        tools = unreal.AssetToolsHelpers.get_asset_tools()
        mi = tools.create_asset(f"MI_{safe}", mat_dest, unreal.MaterialInstanceConstant,
                                unreal.MaterialInstanceConstantFactoryNew())
        if mi:
            mi.set_editor_property("parent", _asset_lib().load_asset(master))
    if not mi:
        return None
    mel = unreal.MaterialEditingLibrary
    bc = props.get("base_color") or [0.5, 0.5, 0.5, 1.0]
    try:
        mel.set_material_instance_vector_parameter_value(
            mi, "BaseColor",
            unreal.LinearColor(bc[0], bc[1], bc[2], bc[3] if len(bc) > 3 else 1.0))
        mel.set_material_instance_scalar_parameter_value(mi, "Metallic", float(props.get("metallic", 0.0)))
        mel.set_material_instance_scalar_parameter_value(mi, "Roughness", float(props.get("roughness", 0.5)))
        mel.set_material_instance_scalar_parameter_value(mi, "Specular", float(props.get("specular", 0.5)))
    except Exception as e:
        _log(f"  color MI param set failed: {e}", "warning")
    _asset_lib().save_asset(mi_path)
    return mi_path


def _apply_materials(obj_data, exchange_folder):
    """Build ONE master + instance per UNIQUE Blender material (deduped by name) and assign them
    per slot to the actors. Material-driven: uses materials.json (material -> textures) + each
    object's ordered slot list. Textures -> Textures/<collection>/T_<mat>_<ch>; materials ->
    Materials/<collection>/{M_,MI_}<mat>. A material shared by N meshes => ONE MI_ reused."""
    if not obj_data or not exchange_folder:
        return {}
    try:
        mat_list = _read_material_manifest(exchange_folder)
    except Exception as e:
        _log(f"  MAT: materials.json read failed: {e}", "warning")
        return {}
    _log(f"  MAT: materials.json has {len(mat_list)} entr(ies)")

    # Only build materials the PLACED actors actually use (each object's slot list). This skips
    # materials.json entries from folded-away LOD levels (e.g. a LOD1 mesh with duplicated
    # 'Material_001.006' slots) — the StaticMesh takes its slots from LOD0, so those are noise.
    needed = set()
    for od in obj_data:
        for m in od.get("materials", []):
            if m:
                needed.add(m)

    # Unique material -> {channel: local texture path} (filename channel, socket as fallback) +
    # color props (for texture-less materials, which build a color MI off the shared master).
    mat_tex = {}
    mat_props = {}
    for entry in mat_list:
        mname = entry.get("name", "")
        if not mname or (needed and mname not in needed):
            continue
        chans = mat_tex.setdefault(mname, {})
        for sock, t in (entry.get("textures") or {}).items():
            path = t.get("path", "")
            if not path:
                continue
            ch = _detect_channel(os.path.basename(path)) or _socket_to_channel(sock)
            if ch and ch not in chans:
                chans[ch] = path
        if mname not in mat_props:
            mat_props[mname] = {
                "base_color": entry.get("base_color") or entry.get("diffuse_color"),
                "metallic": entry.get("metallic", 0.0),
                "roughness": entry.get("roughness", 0.5),
                "specular": entry.get("specular", entry.get("specular_ior_level", 0.5)),
            }

    # Material -> collection (first object that references it) for folder routing.
    mat_coll = {}
    for od in obj_data:
        coll = od.get("collection", "")
        for mname in od.get("materials", []):
            if mname and mname not in mat_coll:
                mat_coll[mname] = coll

    mel = unreal.MaterialEditingLibrary
    mi_by_mat = {}
    tex_master_cache = {}   # channel-set -> shared master path (compile once, reuse for all MI_)
    for mname, chans in mat_tex.items():
        coll = mat_coll.get(mname, "")
        safe = _sanitize_seg(mname)
        mat_dest = _collection_folder(_mat_dir(), coll)
        mi_path = None

        if chans:
            # Textured: import textures, build per-material master M_ + instance MI_.
            tex_dest = _collection_folder(_tex_dir(), coll)
            _log(f"  MAT '{mname}': textured {sorted(chans.keys())} -> {mat_dest}")
            uchan = {}
            for ch, local in chans.items():
                tp = _import_tex(local, tex_dest, f"T_{safe}_{ch}")
                if tp:
                    _config_tex(tp, ch)
                    uchan[ch] = tp
                else:
                    _log(f"    texture import failed: {ch} <- {local}", "warning")
            if uchan:
                master = _ensure_tex_master(uchan, mel, tex_master_cache)
                mi_path = _create_mi(f"MI_{safe}", mat_dest, master, uchan)
            else:
                _log(f"  MAT '{mname}': texture imports failed — color fallback", "warning")
                mi_path = _color_mi(mname, safe, mat_dest, mat_props.get(mname, {}))
        else:
            # Color-only: MI_ off the shared MM_BlenderBridge master.
            _log(f"  MAT '{mname}': color-only -> {mat_dest}")
            mi_path = _color_mi(mname, safe, mat_dest, mat_props.get(mname, {}))

        if mi_path:
            mi_by_mat[mname] = mi_path

    if not mi_by_mat:
        return {}

    # Assign MI_ per slot, on BOTH the component (per actor) AND the StaticMesh asset default
    # (so the asset itself uses MI_ — the embed auto-materials become unreferenced and can be
    # cleaned). The SM is set once (instanced actors share it).
    asub = unreal.get_editor_subsystem(unreal.EditorActorSubsystem)
    assigned = 0
    sm_done = set()
    for od in obj_data:
        guid = od.get("guid", "")
        actor = _find_actor_by_guid(asub, guid)
        comp = actor.static_mesh_component if actor else None
        if not comp:
            continue
        mats = [(_asset_lib().load_asset(mi_by_mat[m]) if m in mi_by_mat else None)
                for m in od.get("materials", [])]
        sm = None
        try:
            sm = comp.get_editor_property("static_mesh")
        except Exception:
            pass
        sp = sm.get_path_name().split(".")[0] if sm else None
        do_sm = bool(sp) and sp not in sm_done
        if do_sm:
            sm_done.add(sp)
        for i, mat in enumerate(mats):
            if not mat:
                continue
            try:
                comp.set_material(i, mat)
                assigned += 1
            except Exception:
                pass
            if do_sm:
                try:
                    sm.set_material(i, mat)
                except Exception:
                    pass
        if do_sm:
            try:
                _asset_lib().save_asset(sp)
            except Exception:
                pass
    _log(f"Materials: {len(mi_by_mat)} unique (master+MI), {assigned} slot(s) assigned")
    return mi_by_mat


# ============================================================
# IMPORT TEXTURES + CREATE MATERIALS
# ============================================================

def _process_textures(tex_folder, scene_name, tex_dest=None, mat_dest=None):
    """Import textures and create PBR materials.

    Args:
        tex_folder: Local folder with texture files
        scene_name: Scene name for asset naming
        tex_dest: UEFN destination for textures (default: _tex_dir(scene_name))
        mat_dest: UEFN destination for materials (default: _mat_dir(scene_name))
    """
    if not tex_folder or not os.path.exists(tex_folder):
        return {}

    if tex_dest is None:
        tex_dest = _tex_dir(scene_name)
    if mat_dest is None:
        mat_dest = _mat_dir(scene_name)

    files = [f for f in os.listdir(tex_folder)
             if f.lower().endswith((".png", ".jpg", ".jpeg", ".tga", ".bmp", ".tiff", ".exr"))]
    if not files:
        return {}

    # Group by texture set
    tex_sets = {}
    for f in files:
        ch = _detect_channel(f)
        if not ch:
            continue
        sn = _get_set_name(f)
        if sn not in tex_sets:
            tex_sets[sn] = {}
        tex_sets[sn][ch] = f

    _log(f"  {len(tex_sets)} texture set(s), {len(files)} file(s)")

    mel = unreal.MaterialEditingLibrary
    created_mats = {}

    for sn, channels in tex_sets.items():
        tex_paths = {}
        for ch, fname in channels.items():
            fp = _exchange_path(Path(tex_folder) / fname, _TEXTURE_SUFFIXES)
            aname = _sanitize_seg(f"T_{scene_name}_{os.path.splitext(fname)[0]}")
            ap = _import_tex(fp, tex_dest, aname)
            if ap:
                _config_tex(ap, ch)
                tex_paths[ch] = ap
                _log(f"    {fname} -> {ch}")

        if not tex_paths:
            continue

        parent_name = _sanitize_seg(f"M_{scene_name}_{sn}")
        parent_path = _create_parent_material(parent_name, mat_dest, tex_paths, mel)
        if not parent_path:
            continue

        mi_name = _sanitize_seg(f"MI_{scene_name}_{sn}")
        mi_path = _create_mi(mi_name, mat_dest, parent_path, tex_paths)
        if mi_path:
            created_mats[sn] = mi_path

    return created_mats

# ============================================================
# COMMAND REGISTRY
# ============================================================

_HANDLERS = {}


def _reg(n):
    def d(fn):
        _HANDLERS[n] = fn
        return fn
    return d


def _dispatch(cmd, par):
    h = _HANDLERS.get(cmd)
    if not h:
        raise ValueError(f"Unknown command: {cmd}")
    if cmd not in {"ping", "get_status", "get_log", "blender_disconnect"}:
        _check_world()
    if cmd in {"import_scene", "add_objects", "update_objects", "update_textures"}:
        par = dict(par)
        if par.get("exchange_folder"):
            par["exchange_folder"] = _exchange_path(par["exchange_folder"], directory=True)
            _read_material_manifest(par["exchange_folder"])
        if par.get("fbx_path"):
            par["fbx_path"] = _exchange_path(par["fbx_path"], (".fbx",))
        if par.get("fbx_paths"):
            par["fbx_paths"] = {
                key: _exchange_path(value, (".fbx",))
                for key, value in par["fbx_paths"].items()
            }
        # This name is sometimes used as an asset path segment.
        par["scene_name"] = _sanitize_seg(par.get("scene_name", "Scene"))
    return h(**par)


@_reg("ping")
def _ping():
    global _last_import
    return {
        "status": "ok",
        "port": _bound_port,
        "project_path": _project_path,
        "blender_connected": bool(_blender_info),
        "total_imports": _total_imports,
    }


@_reg("get_log")
def _get_log(last_n=50):
    return {"lines": _log_ring[-last_n:]}


@_reg("get_status")
def _get_status():
    return {
        "port": _bound_port,
        "project_path": _project_path,
        "bridge_project": _bridge_project,
        "blender": _blender_info,
        "total_imports": _total_imports,
        "scenes": list(_imported_scenes.keys()),
    }


@_reg("blender_connect")
def _blender_connect(blender_version="", addon_version="", project_name="",
                     server_port=0, **kw):
    global _blender_info, _bridge_project, _blender_server_port
    # Folder name = the Blender .blend name (sent by Blender). Sanitize for a valid asset path
    # (a space/dot would break it). This is the BLENDER project, not the UEFN project.
    _bridge_project = _sanitize_seg(project_name.strip()) or "Default"
    _blender_server_port = int(server_port) if server_port else 0
    if not 0 <= _blender_server_port <= 65535:
        raise ValueError("Invalid Blender callback port.")
    _blender_info = {
        "version": blender_version,
        "addon_version": addon_version,
        "project_name": _bridge_project,
        "connected_at": time.time(),
        "server_port": _blender_server_port,
    }
    _log(f"Blender connected: v{blender_version}, project: {_bridge_project}")
    if _blender_server_port:
        _log(f"  Blender server: :{_blender_server_port}")
    _log(f"  Meshes:    {_mesh_dir()}")
    _log(f"  Materials: {_mat_dir()}")
    _log(f"  Textures:  {_tex_dir()}")
    owned = _owned_actors(unreal.get_editor_subsystem(unreal.EditorActorSubsystem))
    return {"status": "ok", "project_path": _project_path,
            "bridge_project": _bridge_project, "guids": sorted(owned)}


@_reg("blender_disconnect")
def _blender_disconnect(**kw):
    global _blender_info
    _blender_info = {}
    _log("Blender disconnected")
    return {"status": "ok"}


@_reg("import_scene")
def _import_scene_cmd(fbx_path="", scene_name="Scene", objects=None,
                      selected_only=False, exchange_folder="",
                      combine_meshes=True, **kw):
    global _last_import, _total_imports

    _log(f"IMPORT: {scene_name} ({'selected' if selected_only else 'full'})")

    mesh_dest = _mesh_dir(scene_name)
    mat_dest = _mat_dir(scene_name)
    tex_dest = _tex_dir(scene_name)
    obj_data = _validated_objects(objects)
    owned = _owned_actors(unreal.get_editor_subsystem(unreal.EditorActorSubsystem))
    if any(od["guid"] in owned for od in obj_data):
        raise ValueError("These actors already exist. Use incremental sync to update them in place.")

    if not fbx_path or not os.path.exists(fbx_path):
        _log(f"FBX not found: {fbx_path}", "error")
        return {"success": False, "error": "FBX not found"}

    imported = _import_fbx(fbx_path, mesh_dest, combine_meshes)
    mesh_paths = []
    for p in imported:
        cls = _asset_class(p)
        if "StaticMesh" in cls:
            mesh_paths.append(p)
        else:
            # Fallback: try loading directly
            try:
                asset = unreal.load_asset(str(p))
                if asset and isinstance(asset, unreal.StaticMesh):
                    mesh_paths.append(p)
                    _log(f"  Fallback load_asset matched: {p}")
            except Exception:
                pass

    _log(f"  Meshes: {len(mesh_paths)} of {len(imported)} imported")

    if not mesh_paths:
        return {"success": False, "error": "Import produced no valid meshes",
                "actors": 0, "meshes": 0, "materials": 0}
    actors = _place_meshes(mesh_paths, obj_data, scene_name)

    # Build/assign per-material MM_/MI_ (deduped) in Materials/<collection>. The FBX import no
    # longer brings embed materials, so MI_ is the sole material source (shared textured masters).
    mat_map = _apply_materials(obj_data, exchange_folder)

    _imported_scenes[scene_name] = {
        "actors": len(actors),
        "meshes": len(mesh_paths),
        "materials": mat_map,
        "time": time.time(),
        "selected_only": selected_only,
    }
    _last_import = time.time()
    _total_imports += 1

    _log(f"IMPORT COMPLETE: {len(actors)} actor(s), {len(mat_map)} material(s)")
    if len(actors) != len(obj_data):
        raise RuntimeError(f"Placed {len(actors)} of {len(obj_data)} bridge actors.")
    return {
        "success": True,
        "actors": len(actors),
        "meshes": len(mesh_paths),
        "materials": len(mat_map),
    }


@_reg("update_transforms")
def _update_transforms_cmd(objects=None, **kw):
    asub = unreal.get_editor_subsystem(unreal.EditorActorSubsystem)
    actors_map = _owned_actors(asub)
    updated = 0
    for od in _validated_objects(objects):
        actor = actors_map.get(od["guid"])
        if not actor:
            continue
        _apply_obj_transform(actor, od)
        updated += 1

    return {"updated": updated}


@_reg("add_objects")
def _add_objects_cmd(fbx_paths=None, objects=None, scene_name="Scene",
                     exchange_folder="", **kw):
    """Import and place new objects without touching existing actors."""
    if not fbx_paths:
        return {"added": 0}

    mesh_dest = _mesh_dir(scene_name)
    obj_data = _validated_objects(objects)
    owned = _owned_actors(unreal.get_editor_subsystem(unreal.EditorActorSubsystem))
    if any(od["guid"] in owned for od in obj_data):
        raise ValueError("Add Objects cannot overwrite existing bridge actors.")

    # Import each rep mesh FBX (one per unique datablock), collect all StaticMeshes, then place
    # ALL objects together so instances can reference a shared mesh (object-driven — F-44).
    all_mesh_paths = []
    for obj_name, fbx_path in fbx_paths.items():
        if not os.path.exists(fbx_path):
            _log(f"  FBX not found for {obj_name}: {fbx_path}", "warning")
            continue
        imported = _import_fbx(fbx_path, mesh_dest, combine=False)
        all_mesh_paths += [p for p in imported if "StaticMesh" in _asset_class(p)]

    if not all_mesh_paths:
        _log("  No meshes imported", "warning")
        return {"added": 0}

    actors = _place_meshes(all_mesh_paths, obj_data, scene_name)
    if len(actors) != len(obj_data):
        raise RuntimeError(f"Placed {len(actors)} of {len(obj_data)} requested bridge actors.")
    materials = _apply_materials(obj_data, exchange_folder)
    if materials:
        _imported_scenes.setdefault(scene_name, {}).setdefault("materials", {}).update(materials)
    _log(f"Added {len(actors)} object(s)")
    return {"added": len(actors)}


@_reg("update_objects")
def _update_objects_cmd(fbx_paths=None, objects=None, scene_name="Scene",
                        exchange_folder="", **kw):
    """Refresh bridge-owned mesh packages while retaining all placed actors."""
    if not fbx_paths:
        return {"updated": 0}

    obj_data = _validated_objects(objects)
    owned = _owned_actors(unreal.get_editor_subsystem(unreal.EditorActorSubsystem))
    mesh_root = _mesh_dir() + "/"
    targets = {}
    for od in obj_data:
        actor = owned.get(od["guid"])
        component = actor.get_component_by_class(unreal.StaticMeshComponent) if actor else None
        asset = component.get_editor_property("static_mesh") if component else None
        if not isinstance(asset, unreal.StaticMesh):
            raise ValueError("An incremental geometry update requires an existing bridge mesh actor.")
        package = asset.get_path_name().split(".")[0]
        if not package.startswith(mesh_root):
            raise ValueError("Cannot update a mesh outside the current bridge project's mesh folder.")
        key = od.get("mesh", od.get("name", ""))
        if key not in fbx_paths:
            raise ValueError(f"No new FBX for bridge mesh {key}.")
        if key in targets and targets[key] != package:
            raise ValueError(f"Bridge instances have inconsistent mesh assignments for {key}.")
        targets[key] = package
    if set(targets) != set(fbx_paths) or len(set(targets.values())) != len(targets):
        raise ValueError("Incremental updates need one source FBX per destination mesh.")

    for key, package in targets.items():
        dest, name = package.rsplit("/", 1)
        reported = {str(path).split(".")[0] for path in
                    _import_fbx(fbx_paths[key], dest, combine=True, existing_name=name)}
        if reported != {package} or not isinstance(unreal.load_asset(package), unreal.StaticMesh):
            raise RuntimeError(f"FBX did not reimport into the expected bridge mesh {package}.")
        if not _asset_lib().save_asset(package):
            raise RuntimeError(f"Cannot save updated bridge mesh {package}.")

    # Importing into the same asset package retains actor identity and instance references.
    _log(f"Reimported {len(targets)} bridge mesh(es) in place")
    return {"updated": len(obj_data), "meshes": len(targets)}


@_reg("update_materials")
def _update_materials_cmd(materials=None, **kw):
    """Update material properties on existing actors without FBX re-import."""
    if not materials:
        return {"updated": 0}

    asub = unreal.get_editor_subsystem(unreal.EditorActorSubsystem)
    actors_map = {}
    for actor in _owned_actors(asub).values():
        label = actor.get_actor_label()
        if label and label.startswith(ACTOR_PREFIX):
            actors_map[label[len(ACTOR_PREFIX):]] = actor

    updated = 0
    for mat_info in materials:
        obj_name = mat_info.get("object_name", "")
        actor = actors_map.get(obj_name)
        if not actor:
            continue

        try:
            comp = actor.get_component_by_class(unreal.StaticMeshComponent)
            if not comp:
                continue

            # Get current material or create overlay
            num_mats = comp.get_num_materials()
            for i in range(max(1, num_mats)):
                existing = comp.get_material(i)
                dmi = comp.create_dynamic_material_instance(
                    i, existing, _sanitize_seg(f"DMI_{obj_name}"))
                if not dmi:
                    continue

                # Set base color
                bc = mat_info.get("base_color", mat_info.get("diffuse_color"))
                if bc and len(bc) >= 3:
                    dmi.set_vector_parameter_value(
                        "Base Color",
                        unreal.LinearColor(bc[0], bc[1], bc[2], bc[3] if len(bc) > 3 else 1.0))

                # Set scalar parameters
                for param, key in [("Metallic", "metallic"),
                                   ("Roughness", "roughness"),
                                   ("Specular", "specular")]:
                    val = mat_info.get(key)
                    if val is not None:
                        dmi.set_scalar_parameter_value(param, float(val))

            updated += 1
        except Exception as e:
            _log(f"  Material update failed for {obj_name}: {e}", "warning")

    _log(f"Updated materials on {updated} actor(s)")
    return {"updated": updated}


@_reg("update_textures")
def _update_textures_cmd(object_names=None, exchange_folder="",
                         scene_name="Scene", **kw):
    """Re-import textures and rebind material instances without FBX re-import."""
    if not object_names:
        return {"updated": 0}

    tex_dest = _tex_dir(scene_name)
    mat_dest = _mat_dir(scene_name)

    # Read material info JSON from exchange folder
    tex_folder = os.path.join(exchange_folder, "textures") if exchange_folder else ""
    if not tex_folder or not os.path.isdir(tex_folder):
        _log("  No texture folder found", "warning")
        return {"updated": 0}

    # Import texture files and build channel mapping
    imported_textures = {}  # {image_name: uefn_asset_path}
    tex_folder = _exchange_path(tex_folder, directory=True)
    for fname in os.listdir(tex_folder):
        if not fname.lower().endswith(_TEXTURE_SUFFIXES):
            continue
        fp = _exchange_path(Path(tex_folder) / fname, _TEXTURE_SUFFIXES)
        aname = _sanitize_seg(f"T_{scene_name}_{os.path.splitext(fname)[0]}")
        ap = _import_tex(fp, tex_dest, aname)
        if ap:
            ch = _detect_channel(fname)
            if ch:
                _config_tex(ap, ch)
            imported_textures[os.path.splitext(fname)[0]] = ap
            # Also map by full filename stem
            img_name = os.path.splitext(fname)[0]
            imported_textures[img_name] = ap
            _log(f"  Texture: {fname} -> {ap}")

    if not imported_textures:
        _log("  No textures imported", "warning")
        return {"updated": 0}

    # Try to find and update existing Material Instances
    scene_state = _imported_scenes.get(scene_name, {})
    existing_mats = scene_state.get("materials", {})
    updated = 0

    if existing_mats:
        # Rebind textures on existing MIs
        for sn, mi_path in existing_mats.items():
            mi = _asset_lib().load_asset(mi_path)
            if not mi:
                continue
            # Build tex_paths for this MI from imported textures
            tex_paths = {}
            for img_name, ap in imported_textures.items():
                ch = _detect_channel(img_name)
                if ch:
                    tex_paths[ch] = ap
            if tex_paths:
                _set_mi_tex(mi, mi_path, tex_paths)
                updated += 1
                _log(f"  Rebound MI: {mi_path}")
    else:
        # No existing MIs — create via full pipeline
        created_mats = _process_textures(tex_folder, scene_name, tex_dest, mat_dest)
        if created_mats:
            # Apply to actors
            asub = unreal.get_editor_subsystem(unreal.EditorActorSubsystem)
            obj_set = set(object_names)
            for actor in _owned_actors(asub).values():
                label = actor.get_actor_label()
                if not label or not label.startswith(ACTOR_PREFIX):
                    continue
                obj_name = label[len(ACTOR_PREFIX):]
                if obj_name in obj_set:
                    first_mat = list(created_mats.values())[0]
                    _apply_material_to_actors([actor], first_mat)
                    updated += 1

            # Store for future rebinds
            if scene_name in _imported_scenes:
                _imported_scenes[scene_name]["materials"] = created_mats
            else:
                _imported_scenes[scene_name] = {"materials": created_mats}

    _log(f"Updated textures on {updated} material(s)")
    return {"updated": updated}


# ============================================================
# BIDIRECTIONAL SYNC — UEFN → Blender
# ============================================================

def _read_bb_transforms():
    """Read transforms only for actors owned by the connected Blender project."""
    _check_world()
    asub = unreal.get_editor_subsystem(unreal.EditorActorSubsystem)
    result = []
    for guid, actor in _owned_actors(asub).items():
        obj_name = actor.get_actor_label().removeprefix(ACTOR_PREFIX)
        loc = actor.get_actor_location()
        rot = actor.get_actor_rotation()
        sc = actor.get_actor_scale3d()
        result.append({
            "name": obj_name,
            "guid": guid,
            "location": [loc.x, loc.y, loc.z],
            "rotation": [rot.pitch, rot.yaw, rot.roll],
            "scale": [sc.x, sc.y, sc.z],
        })
    return result


def _dirty_map_count():
    """Number of dirty map packages. Under OFPA/World Partition a moved actor dirties its own
    external (__ExternalActors__) package, which counts as a map package; saving clears it — so
    a DROP in this count means a save happened. This is the only dirty API this UEFN build
    exposes to Python (actor.get_package/get_outermost raise AttributeError here)."""
    try:
        return len(unreal.EditorLoadingAndSavingUtils.get_dirty_map_packages())
    except Exception:
        return -1


def _transform_hash(t):
    """Hash a single transform dict for diff comparison."""
    loc = t.get("location", [0, 0, 0])
    rot = t.get("rotation", [0, 0, 0])
    sc = t.get("scale", [1, 1, 1])
    # Round to avoid float noise (0.01 precision is ~1mm in UE units)
    key = "|".join(f"{v:.2f}" for v in loc + rot + sc)
    return key


def _snapshot_transforms(transforms):
    return {t["guid"]: _transform_hash(t) for t in transforms}


def _diff_transforms(transforms):
    """Return only transforms that changed since last push. Updates snapshot."""
    global _last_push_snapshot
    new_snap = _snapshot_transforms(transforms)
    changed = []
    for t in transforms:
        guid = t["guid"]
        if new_snap[guid] != _last_push_snapshot.get(guid):
            changed.append(t)
    _last_push_snapshot = new_snap
    return changed


def _push_to_blender(port=0, transforms=None):
    """HTTP POST transforms to Blender's server."""
    if not port:
        port = _blender_server_port
    if not port or int(port) != _blender_server_port:
        _log("No Blender server port — cannot push", "warning")
        return False
    if not transforms:
        _log("No transforms to push", "warning")
        return False

    payload = json.dumps({
        "command": "push_transforms",
        "params": {"objects": transforms},
    }).encode("utf-8")

    req = urllib.request.Request(
        f"http://127.0.0.1:{port}",
        data=payload,
        headers={"Content-Type": "application/json", "X-Bridge-Token": _session_token},
        method="POST",
    )
    try:
        with _loopback_opener.open(req, timeout=10) as resp:
            data = resp.read(MAX_REQUEST_BYTES + 1)
            if len(data) > MAX_REQUEST_BYTES:
                raise ValueError("Blender returned an oversized response.")
            body = json.loads(data.decode("utf-8"))
            if body.get("success"):
                _log(f"Pushed {len(transforms)} transform(s) to Blender")
                return True
            else:
                _log(f"Blender rejected push: {body.get('error', '?')}", "warning")
                return False
    except Exception as e:
        _log(f"Push to Blender failed: {e}", "error")
        return False


def _refresh_push_snapshot():
    """Reset the push baseline to the current actor transforms. Called after applying
    inbound changes from Blender so the live poll does NOT bounce them straight back."""
    global _last_push_snapshot
    if not _live_sync_active:
        return
    try:
        _last_push_snapshot = _snapshot_transforms(_read_bb_transforms())
    except Exception:
        pass


@_reg("set_live_sync")
def _set_live_sync_cmd(active=False, blender_port=0, **kw):
    """Blender toggles Live Sync — when on, UEFN pushes actor edits back to Blender on each
    UEFN save (Ctrl+S). Carries Blender's server port so a toggle re-establishes it even after
    the UEFN script was re-run (which resets these globals). Baseline reset on enable."""
    global _live_sync_active, _last_push_snapshot
    if blender_port and int(blender_port) != _blender_server_port:
        raise ValueError("Reconnect Blender before changing its callback port.")
    _live_sync_active = bool(active)
    if _live_sync_active:
        try:
            _last_push_snapshot = _snapshot_transforms(_read_bb_transforms())
        except Exception:
            _last_push_snapshot = {}
        _log(f"Live Sync ON — push on Ctrl+S (port {_blender_server_port}, {_dirty_map_count()} dirty)")
    else:
        _log("Live Sync OFF")
    return {"live": _live_sync_active, "blender_port": _blender_server_port}


@_reg("request_push_transforms")
def _request_push_transforms_cmd(blender_port=0, **kw):
    """Blender requests UEFN to push current actor transforms back (full sync)."""
    global _last_push_snapshot
    port = int(blender_port) if blender_port else _blender_server_port
    if not port:
        return {"error": "No Blender server port"}

    transforms = _read_bb_transforms()
    if not transforms:
        return {"pushed": 0, "message": "No BB_ actors found"}

    ok = _push_to_blender(port, transforms)
    if ok:
        _last_push_snapshot = _snapshot_transforms(transforms)
    return {"pushed": len(transforms) if ok else 0}


@_reg("push_transforms_to_blender")
def _push_transforms_to_blender_cmd(**kw):
    """Dashboard button: push all BB_ transforms to Blender (full sync)."""
    global _last_push_snapshot
    if not _blender_server_port:
        _log("Blender server port unknown — connect from Blender first", "warning")
        return {"error": "No Blender server port"}

    transforms = _read_bb_transforms()
    if not transforms:
        _log("No BB_ actors to push", "warning")
        return {"pushed": 0}

    ok = _push_to_blender(_blender_server_port, transforms)
    if ok:
        _last_push_snapshot = _snapshot_transforms(transforms)
    return {"pushed": len(transforms) if ok else 0}


# ============================================================
# HTTP SERVER
# ============================================================

class _H(BaseHTTPRequestHandler):
    def _allowed(self):
        if self.path != "/" or self.headers.get("Origin") or self.headers.get("Sec-Fetch-Site"):
            self._e(403, "This endpoint only accepts the local bridge client.")
            return False
        supplied = self.headers.get("X-Bridge-Token", "")
        if not re.fullmatch(r"[0-9a-fA-F]{64}", supplied) or not hmac.compare_digest(supplied, _session_token):
            self._e(401, "Bridge session token required.")
            return False
        return True

    def do_GET(self):
        if not self._allowed():
            return
        b = json.dumps({"status": "ok", "port": _bound_port}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_POST(self):
        global _req_counter
        if not self._allowed():
            return
        if self.headers.get_content_type() != "application/json" or self.headers.get("Transfer-Encoding"):
            self._e(415, "JSON body with Content-Length required.")
            return
        lengths = self.headers.get_all("Content-Length", [])
        if len(lengths) != 1 or not re.fullmatch(r"[0-9]{1,10}", lengths[0]):
            self._e(411, "Valid Content-Length required.")
            return
        length = int(lengths[0])
        if not 0 < length <= MAX_REQUEST_BYTES:
            self._e(413, "Bridge body exceeds the permitted request size.")
            return
        try:
            self.connection.settimeout(15)
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise ValueError("Truncated body.")
            body = json.loads(raw)
            if not isinstance(body, dict):
                raise ValueError("Expected an object.")
        except (ValueError, TimeoutError, UnicodeError):
            self._e(400, "Bad JSON")
            return
        cmd = body.get("command", "")
        par = body.get("params", {})
        if not isinstance(cmd, str) or cmd not in _HANDLERS or not isinstance(par, dict):
            self._e(400, "Unsupported bridge command or parameters.")
            return
        if cmd == "ping":
            b = json.dumps({"success": True, "result": {
                "status": "ok", "port": _bound_port,
                "project_path": _project_path,
                "blender_connected": bool(_blender_info),
            }}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
            return

        _req_counter += 1
        rid = f"r_{_req_counter}_{time.time_ns()}"
        _command_queue.put((rid, cmd, par))
        dl = time.time() + HTTP_TIMEOUT
        while time.time() < dl:
            with _responses_lock:
                if rid in _responses:
                    res = _responses.pop(rid)
                    break
            time.sleep(POLL_INTERVAL)
        else:
            self._e(504, "Timeout")
            return

        b = json.dumps(res).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_OPTIONS(self):
        self._e(403, "Cross-origin requests are disabled.")

    def _e(self, c, m):
        b = json.dumps({"success": False, "error": m}).encode()
        self.send_response(c)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def log_message(self, *a):
        pass


def _start_http():
    global _http_server, _http_thread, _bound_port
    if _http_server:
        return _bound_port
    for p in range(DEFAULT_PORT, MAX_PORT + 1):
        try:
            _http_server = HTTPServer(("127.0.0.1", p), _H)
            _bound_port = p
            _http_thread = threading.Thread(target=_http_server.serve_forever, daemon=True)
            _http_thread.start()
            _log(f"HTTP server on :{p}")
            return p
        except OSError:
            continue
    raise RuntimeError("No free port in range")


def _stop_http():
    global _http_server, _http_thread, _bound_port
    if not _http_server:
        return
    server = _http_server
    _http_server = None
    _http_thread = None
    _bound_port = 0
    # Avoid blocking Slate while a request is waiting for the main-thread queue.
    def close():
        server.shutdown()
        server.server_close()

    threading.Thread(target=close, daemon=True).start()

# ============================================================
# TKINTER DASHBOARD
# ============================================================

import tkinter as tk


class Dashboard:
    def __init__(self, port):
        self.port = port
        self.root = tk.Tk()
        self.root.title(f"Blender Bridge :{port}")
        self.root.geometry("480x780")
        self.root.configure(bg="#0d1117")
        self.root.attributes("-topmost", True)

        # Theme
        self.BG = "#0d1117"
        self.BG2 = "#161b22"
        self.BG3 = "#21262d"
        self.FG = "#e6edf3"
        self.DIM = "#8b949e"
        self.ACC = "#58a6ff"
        self.GRN = "#3fb950"
        self.RED = "#f85149"
        self.ORG = "#d29922"
        self.PRP = "#bc8cff"
        self.BRD = "#30363d"

        self.tick_h = None
        self.fc = 0
        self._sh = ""
        self._poll_next = 0.0        # debounce for the UEFN->Blender save check
        self._dirty_n = 0            # prior dirty map-package count (save = count drops)

        self._build()
        self._on_set()
        self._start_tick()

    def _build(self):
        bg = self.BG

        # Header
        h = tk.Frame(self.root, bg=bg)
        h.pack(fill="x", padx=16, pady=(12, 0))
        tk.Label(h, text="BLENDER", font=("Segoe UI", 14, "bold"),
                 fg=self.ACC, bg=bg).pack(side="left")
        tk.Label(h, text="  \u2194  ", font=("Segoe UI", 14),
                 fg=self.BRD, bg=bg).pack(side="left")
        tk.Label(h, text="UEFN", font=("Segoe UI", 14, "bold"),
                 fg=self.PRP, bg=bg).pack(side="left")
        self.dot = tk.Label(h, text="\u25cf", font=("Segoe UI", 16),
                            fg=self.DIM, bg=bg)
        self.dot.pack(side="right")
        self.clbl = tk.Label(h, text="Waiting...", font=("Segoe UI", 8),
                             fg=self.DIM, bg=bg)
        self.clbl.pack(side="right", padx=(0, 6))

        tk.Frame(self.root, bg=self.BRD, height=1).pack(fill="x", padx=16, pady=6)

        # UEFN project (auto-detected — read-only, no field to configure)
        pp = tk.Frame(self.root, bg=bg)
        pp.pack(fill="x", padx=16)
        tk.Label(pp, text="UEFN PROJECT", font=("Segoe UI", 7, "bold"),
                 fg=self.DIM, bg=bg).pack(side="left")
        self.pv = tk.StringVar(value=_project_path or "(detecting...)")
        tk.Label(pp, textvariable=self.pv, font=("Consolas", 10),
                 fg=self.PRP, bg=bg).pack(side="left", padx=(8, 0))

        # Bridge project name display
        bp = tk.Frame(self.root, bg=bg)
        bp.pack(fill="x", padx=16, pady=(4, 0))
        tk.Label(bp, text="BRIDGE PROJECT", font=("Segoe UI", 7, "bold"),
                 fg=self.DIM, bg=bg).pack(side="left")
        self.bp_v = tk.StringVar(value=_bridge_project or "Waiting...")
        tk.Label(bp, textvariable=self.bp_v, font=("Consolas", 10),
                 fg=self.ACC, bg=bg).pack(side="left", padx=(8, 0))

        # Settings
        sf = tk.Frame(self.root, bg=bg)
        sf.pack(fill="x", padx=16, pady=(6, 0))
        tk.Label(sf, text="Scale:", fg=self.DIM, bg=bg,
                 font=("Segoe UI", 8)).pack(side="left", padx=(0, 4))
        self.sv = tk.StringVar(value="1.0")
        se = tk.Entry(sf, textvariable=self.sv, bg="#0d1117", fg=self.FG,
                      insertbackground=self.FG, font=("Consolas", 9),
                      relief="flat", width=5, highlightbackground=self.BRD,
                      highlightthickness=1)
        se.pack(side="left", ipady=2)

        tk.Frame(self.root, bg=self.BRD, height=1).pack(fill="x", padx=16, pady=6)

        # Status cards
        cs = tk.Frame(self.root, bg=bg)
        cs.pack(fill="x", padx=16)
        r1 = tk.Frame(cs, bg=bg)
        r1.pack(fill="x", pady=2)
        self._scard(r1, "LISTENER", f":{self.port}", self.GRN, "left")
        self.bv = tk.StringVar(value="Waiting...")
        self._vcard(r1, "BLENDER", self.bv, "right")
        r2 = tk.Frame(cs, bg=bg)
        r2.pack(fill="x", pady=2)
        self.li_v = tk.StringVar(value="\u2014")
        self._vcard(r2, "LAST IMPORT", self.li_v, "left")
        self.ti_v = tk.StringVar(value="0")
        self._vcard(r2, "TOTAL IMPORTS", self.ti_v, "right")

        tk.Frame(self.root, bg=self.BRD, height=1).pack(fill="x", padx=16, pady=6)

        # Imported scenes
        mh = tk.Frame(self.root, bg=bg)
        mh.pack(fill="x", padx=16)
        tk.Label(mh, text="IMPORTED SCENES", font=("Segoe UI", 8, "bold"),
                 fg=self.DIM, bg=bg).pack(side="left")
        self.sc = tk.Label(mh, text="0", font=("Segoe UI", 8, "bold"),
                           fg=self.ACC, bg=bg)
        self.sc.pack(side="right")
        self.sf = tk.Frame(self.root, bg=bg)
        self.sf.pack(fill="x", padx=16, pady=4)
        self._wait_msg()

        tk.Frame(self.root, bg=self.BRD, height=1).pack(fill="x", padx=16, pady=6)

        # Actions
        tk.Label(self.root, text="ACTIONS", font=("Segoe UI", 8, "bold"),
                 fg=self.DIM, bg=bg).pack(padx=16, anchor="w")
        ab = tk.Frame(self.root, bg=bg)
        ab.pack(fill="x", padx=16, pady=6)
        self._btn(ab, "Apply to Selected", self._apply, "#1f6feb")

        # Sync actions
        sb = tk.Frame(self.root, bg=bg)
        sb.pack(fill="x", padx=16, pady=(0, 6))
        self._btn(sb, "Push to Blender", self._push_bl, self.PRP)

        tk.Frame(self.root, bg=self.BRD, height=1).pack(fill="x", padx=16, pady=6)

        # Log
        lh = tk.Frame(self.root, bg=bg)
        lh.pack(fill="x", padx=16)
        tk.Label(lh, text="ACTIVITY", font=("Segoe UI", 8, "bold"),
                 fg=self.DIM, bg=bg).pack(side="left")
        tk.Button(lh, text="Clear", font=("Segoe UI", 7), fg=self.DIM,
                  bg=bg, relief="flat", bd=0, cursor="hand2",
                  command=self._cl).pack(side="right")
        self.log = tk.Text(self.root, height=6, bg=self.BG2, fg=self.DIM,
                           font=("Consolas", 8), relief="flat", state="disabled",
                           highlightthickness=0, padx=8, pady=6)
        self.log.pack(fill="both", expand=True, padx=16, pady=(4, 4))
        self.log.tag_configure("error", foreground=self.RED)
        self.log.tag_configure("sync", foreground=self.GRN)

        # Footer
        ft = tk.Frame(self.root, bg=self.BG2, height=24)
        ft.pack(fill="x", side="bottom")
        ft.pack_propagate(False)
        tk.Label(ft, text="by KiKoZl - Surprise Co.",
                 font=("Segoe UI", 7), fg="#484f58", bg=self.BG2).pack(side="left", padx=8)
        tk.Label(ft, text="github.com/KiKoZl1",
                 font=("Segoe UI", 7), fg="#484f58", bg=self.BG2).pack(side="right", padx=8)

    # --- Widgets ---

    def _scard(self, p, l, v, c, s):
        f = tk.Frame(p, bg=self.BG2, highlightbackground=self.BRD, highlightthickness=1)
        f.pack(side=s, expand=True, fill="x", padx=2, ipady=6)
        tk.Label(f, text=l, font=("Segoe UI", 7, "bold"), fg=self.DIM, bg=self.BG2).pack()
        tk.Label(f, text=v, font=("Segoe UI", 11, "bold"), fg=c, bg=self.BG2).pack()

    def _vcard(self, p, l, v, s):
        f = tk.Frame(p, bg=self.BG2, highlightbackground=self.BRD, highlightthickness=1)
        f.pack(side=s, expand=True, fill="x", padx=2, ipady=6)
        tk.Label(f, text=l, font=("Segoe UI", 7, "bold"), fg=self.DIM, bg=self.BG2).pack()
        tk.Label(f, textvariable=v, font=("Segoe UI", 11, "bold"), fg=self.FG, bg=self.BG2).pack()

    def _btn(self, p, t, c, bg, fg=None):
        b = tk.Button(p, text=t, font=("Segoe UI", 8, "bold"), bg=bg,
                      fg=fg or "#fff", relief="flat", cursor="hand2",
                      command=c, padx=8, pady=4)
        b.pack(side="left", expand=True, fill="x", padx=2)
        o = bg
        b.bind("<Enter>", lambda e: b.configure(bg=self._li(o)))
        b.bind("<Leave>", lambda e: b.configure(bg=o))

    def _li(self, h):
        try:
            r, g, b = int(h[1:3], 16), int(h[3:5], 16), int(h[5:7], 16)
            return f"#{min(255,r+25):02x}{min(255,g+25):02x}{min(255,b+25):02x}"
        except Exception:
            return h

    def _wait_msg(self):
        for w in self.sf.winfo_children():
            w.destroy()
        tk.Label(self.sf, text="Waiting for Blender...\nInstall the addon and click Connect",
                 fg=self.DIM, bg=self.BG, font=("Segoe UI", 8),
                 justify="left").pack(pady=8)

    # --- Callbacks ---

    def _on_set(self):
        global _import_scale
        try:
            _import_scale = float(self.sv.get())
        except Exception:
            _import_scale = 1.0

    def _on_log(self, e, l="info"):
        try:
            self.log.configure(state="normal")
            tag = "error" if l == "error" else ("sync" if "COMPLETE" in e else "")
            self.log.insert("end", e + "\n", tag)
            self.log.see("end")
            self.log.configure(state="disabled")
        except Exception:
            pass

    def _cl(self):
        self.log.configure(state="normal")
        self.log.delete("1.0", "end")
        self.log.configure(state="disabled")

    def _apply(self):
        _check_world()
        if not _imported_scenes:
            _log("No imported scenes", "warning")
            return
        actors = unreal.get_editor_subsystem(unreal.EditorActorSubsystem).get_selected_level_actors()
        if not actors:
            _log("No selection", "warning")
            return
        scene = list(_imported_scenes.values())[-1]
        mats = scene.get("materials", {})
        if not mats:
            _log("No materials to apply", "warning")
            return
        fm = _asset_lib().load_asset(list(mats.values())[0])
        if not fm:
            return
        c = 0
        for a in actors:
            try:
                if not _actor_guid(a):
                    continue
                comp = a.static_mesh_component
                if comp:
                    for i in range(comp.get_num_materials()):
                        comp.set_material(i, fm)
                    c += 1
            except Exception:
                pass
        _log(f"Applied to {c} actor(s)")

    def _push_bl(self):
        _command_queue.put((f"g_{time.time_ns()}", "push_transforms_to_blender", {}))
        _log("Push to Blender requested")

    # --- Update ---

    def _upd(self):
        now = time.time()

        # Blender status
        if _blender_info:
            ca = _blender_info.get("connected_at", 0)
            if now - ca < 3600:
                self.dot.configure(fg=self.GRN)
                self.clbl.configure(text="Connected", fg=self.GRN)
                v = _blender_info.get("version", "?")
                self.bv.set(f"v{v}")
            else:
                self.dot.configure(fg=self.ORG)
                self.clbl.configure(text="Idle", fg=self.ORG)
                self.bv.set("Idle")
            self.bp_v.set(_bridge_project or "Default")
        else:
            self.dot.configure(fg=self.DIM)
            self.clbl.configure(text="Waiting...", fg=self.DIM)
            self.bv.set("Waiting...")
            self.bp_v.set("Waiting...")
        # Keep the auto-detected UEFN project shown (read-only).
        self.pv.set(_project_path or "(detecting...)")

        # Import stats
        if _last_import > 0:
            e = int(now - _last_import)
            self.li_v.set(f"{e}s ago" if e < 60 else f"{e // 60}m ago")
        self.ti_v.set(str(_total_imports))
        self.sc.configure(text=str(len(_imported_scenes)))

        # Rebuild scene list if changed
        h = str([(n, s.get("actors", 0)) for n, s in _imported_scenes.items()])
        if h != self._sh:
            self._sh = h
            self._reb()

    def _reb(self):
        for w in self.sf.winfo_children():
            w.destroy()
        if not _imported_scenes:
            self._wait_msg()
            return
        for name, state in _imported_scenes.items():
            card = tk.Frame(self.sf, bg=self.BG2,
                            highlightbackground=self.BRD, highlightthickness=1)
            card.pack(fill="x", pady=2)
            r1 = tk.Frame(card, bg=self.BG2)
            r1.pack(fill="x", padx=8, pady=(6, 2))
            tk.Label(r1, text="\u25cf", fg=self.GRN, bg=self.BG2,
                     font=("Segoe UI", 9)).pack(side="left")
            tk.Label(r1, text=f" {name}", fg=self.FG, bg=self.BG2,
                     font=("Segoe UI", 9, "bold")).pack(side="left")
            ac = state.get("actors", 0)
            tk.Label(r1, text=f"{ac} actor{'s' if ac != 1 else ''}",
                     fg=self.DIM, bg=self.BG2,
                     font=("Segoe UI", 7)).pack(side="right")

            # Channel badges
            r2 = tk.Frame(card, bg=self.BG2)
            r2.pack(fill="x", padx=8, pady=(0, 2))
            mats = state.get("materials", {})
            if mats:
                tk.Label(r2, text=f"{len(mats)} material(s)",
                         fg=self.DIM, bg=self.BG2,
                         font=("Segoe UI", 7)).pack(side="left")
            if state.get("baked"):
                tk.Label(r2, text=" BAKED ", fg="#fff", bg=self.ORG,
                         font=("Segoe UI", 6, "bold"), padx=3).pack(side="left", padx=1)

            r3 = tk.Frame(card, bg=self.BG2)
            r3.pack(fill="x", padx=8, pady=(0, 6))
            nm = state.get("meshes", 0)
            tk.Label(r3, text=f"{nm} mesh(es)",
                     fg=self.DIM, bg=self.BG2,
                     font=("Segoe UI", 7)).pack(side="left")

    # --- Tick ---

    def _start_tick(self):
        def tick(dt):
            try:
                if not self.root.winfo_exists():
                    self._stop()
                    return

                n = 0
                wrote_transforms = False
                while not _command_queue.empty() and n < TICK_BATCH:
                    try:
                        rid, cmd, par = _command_queue.get_nowait()
                    except queue.Empty:
                        break
                    try:
                        res = _dispatch(cmd, par)
                        resp = {"success": True, "result": _serialize(res)}
                    except Exception as e:
                        _log(f"'{cmd}' failed: {e}\n{traceback.format_exc()}", "error")
                        resp = {"success": False, "error": str(e)}
                    if cmd in INBOUND_WRITE_CMDS:
                        wrote_transforms = True
                    with _responses_lock:
                        _responses[rid] = resp
                    n += 1

                # A Blender-originated write moved/placed actors — absorb it into the push
                # baseline so the live poll below doesn't echo it straight back to Blender.
                if wrote_transforms and _live_sync_active:
                    _refresh_push_snapshot()

                # Clean stale responses
                now = time.time()
                with _responses_lock:
                    for k in [k for k in _responses
                              if float(k.split("_")[-1]) / 1e9 < now - STALE_SEC]:
                        del _responses[k]

                # Live Sync (UEFN -> Blender): push on SAVE (Ctrl+S), mirroring Blender->UEFN —
                # not on every drag (that lagged). A save is the dirty->clean transition of the
                # BB_ actors' own packages (World-Partition safe). Debounced via _poll_next.
                if (_live_sync_active and _blender_server_port
                        and now > self._poll_next):
                    self._poll_next = now + LIVE_POLL_INTERVAL
                    try:
                        n_dirty = _dirty_map_count()
                        if n_dirty >= 0 and n_dirty != self._dirty_n:
                            if n_dirty > self._dirty_n:
                                _log(f"UEFN edits detected ({n_dirty} unsaved) — push on Ctrl+S")
                            else:
                                # count dropped = packages were saved
                                all_transforms = _read_bb_transforms()
                                changed = _diff_transforms(all_transforms) if all_transforms else []
                                if changed:
                                    _log(f"Saved — pushing {len(changed)} change(s) to Blender")
                                    _push_to_blender(_blender_server_port, changed)
                                else:
                                    _log("Saved — no Blender-relevant changes")
                            self._dirty_n = n_dirty
                    except Exception:
                        pass

                self.fc += 1
                if self.fc % 30 == 0:
                    self._upd()
                self.root.update()
            except tk.TclError:
                self._stop()

        self.tick_h = unreal.register_slate_post_tick_callback(tick)
        _log("Dashboard ready")

    def _stop(self):
        if self.tick_h:
            unreal.unregister_slate_post_tick_callback(self.tick_h)
            self.tick_h = None
        _stop_http()
        _log("Dashboard closed")

# ============================================================
# STARTUP
# ============================================================

def _stop_bridge():
    global _gui
    if _gui:
        dashboard, _gui = _gui, None
        dashboard._stop()
        try:
            if dashboard.root.winfo_exists():
                dashboard.root.destroy()
        except tk.TclError:
            pass
    else:
        _stop_http()


_load_session()
_detect_project_path()
port = _start_http()
try:
    _gui = Dashboard(port)
except Exception:
    _stop_http()
    raise
