"""UEFN Blender Bridge — Blender Side.

Blender addon that exports FBX + textures to UEFN via HTTP bridge.
Supports full scenes, selected objects, and save-triggered sync.

Developed by KiKoZl (Surprise Co.)

github.com/KiKoZl1 | Surprise Co. (surpriseugc.com)
"""

from contextlib import contextmanager
import bpy
import hashlib
import json
import math
import mathutils
import os
from pathlib import Path
import queue as _queue_mod
import re
import struct
import threading
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.error import URLError
from bpy.app.handlers import persistent

from . import coords  # pure Blender<->UEFN convention (single source, unit-tested)
from . import session

# ============================================================
# CONFIG
# ============================================================

UEFN_PORT = 8790
BLENDER_SERVER_PORT = 8791
BLENDER_MAX_PORT = 8795
SERVER_TIMEOUT = 5.0
ADDON_VERSION = "1.0.2"

# ============================================================
# STATE
# ============================================================


class BridgeState:
    connected = False
    port = UEFN_PORT
    status = "Disconnected"
    last_error = ""            # last error message, surfaced in the panel until next action
    last_obj_hashes = {}       # {name: {"geo","mat","tex","trans"}}
    live_active = False
    sending = False            # True while an async send is in flight (one at a time)
    send_count = 0
    project_path = ""
    project_name = ""          # bridge project name (folder in UEFN)
    blend_path = None
    token = ""
    exchange = None
    live_scene = None
    synced_guids = frozenset()
    generation = 0
    pending_save = False
    # Bidirectional sync
    receiving = False          # True when applying inbound changes (suppresses outbound)
    server_port = 0            # Blender HTTP server port
    _http_server = None
    _http_thread = None


_st = BridgeState()

# ============================================================
# LOG
# ============================================================


def _log(m):
    print(f"[UEFNBridge] {m}")


def _err(m):
    _st.last_error = str(m)
    print(f"[UEFNBridge ERROR] {m}")

# ============================================================
# HTTP CLIENT
# ============================================================


def _send(cmd, params=None, timeout=120.0, endpoint=None):
    """Send command to UEFN bridge server."""
    port, token = endpoint or (_st.port, _st.token)
    try:
        body = session.request(port, token, {"command": cmd, "params": params or {}}, timeout)
    except URLError as e:
        if "Connection refused" in str(e) or "No connection" in str(e):
            raise ConnectionError("UEFN not reachable") from e
        raise
    if body.get("success") is not True:
        raise RuntimeError(f"UEFN: {body.get('error', '?')}")
    result = body.get("result", {})
    if not isinstance(result, dict):
        raise RuntimeError("UEFN returned an invalid bridge result.")
    if result.get("error") or result.get("success") is False:
        raise RuntimeError(f"UEFN: {result.get('error') or 'command failed'}")
    return result


def _send_async(cmd, params=None, label="Sent", snapshot=None):
    """Keep asynchronous imports off Blender's main thread and apply acknowledgments there."""
    _st.sending = True
    _st.status = "Sending to UEFN..."
    generation = _st.generation
    endpoint = _st.port, _st.token
    objects = params.get("objects", [])

    def _worker():
        try:
            result = _send(cmd, params, timeout=600.0, endpoint=endpoint)
            _incoming_queue.put(("send_done", (generation, label, result, snapshot, objects)))
        except Exception as e:
            _incoming_queue.put(("send_failed", (generation, str(e))))

    threading.Thread(target=_worker, daemon=True).start()


def _alive():
    """Check if UEFN bridge is running."""
    try:
        return session.request(_st.port, _st.token).get("status") == "ok"
    except Exception:
        return False

# ============================================================
# HTTP SERVER (receives pushes from UEFN)
# ============================================================

_incoming_queue = _queue_mod.Queue()
_transform_queue = _queue_mod.Queue(maxsize=16)


class _BlenderHTTPHandler(BaseHTTPRequestHandler):
    """Handle incoming commands from UEFN."""

    def setup(self):
        self.request.settimeout(SERVER_TIMEOUT)
        super().setup()

    def _allowed(self):
        return self.path == "/" and session.is_authenticated(self.headers, _st.token)

    def do_GET(self):
        if not self._allowed():
            self._err(403, "Bridge authentication required")
            return
        self._ok({"status": "ok", "port": _st.server_port})

    def do_POST(self):
        if not self._allowed():
            self._err(403, "Bridge authentication required")
            return

        try:
            lengths = self.headers.get_all("Content-Length", [])
            if len(lengths) != 1 or not re.fullmatch(r"[0-9]{1,10}", lengths[0]):
                self._err(411, "Valid Content-Length required")
                return
            length = int(lengths[0])
            if not 0 < length <= session.MAX_BODY:
                self._err(413, "Invalid bridge request size")
                return
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise ValueError("Incomplete bridge request.")
            body = session.decode_json(raw)
        except (OSError, ValueError, RecursionError):
            self._err(400, "Invalid bridge JSON")
            return

        cmd = body.get("command", "")
        params = body.get("params", {})

        if cmd == "push_transforms":
            if not _st.connected or not isinstance(params, dict):
                self._err(409, "No connected Blender file")
                return
            try:
                _transform_queue.put_nowait((_st.generation, params))
                self._ok({"status": "queued"})
            except _queue_mod.Full:
                self._err(429, "Blender is processing earlier transforms")
        elif cmd == "ping":
            self._ok({"status": "ok", "port": _st.server_port})
        else:
            self._err(400, f"Unknown command: {cmd}")

    def _ok(self, data):
        b = json.dumps({"success": True, "result": data}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def _err(self, code, msg):
        b = json.dumps({"success": False, "error": msg}).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def log_message(self, *args):
        pass


def _start_blender_server():
    """Start Blender's HTTP server for receiving UEFN pushes."""
    if _st._http_server:
        return _st.server_port
    for p in range(BLENDER_SERVER_PORT, BLENDER_MAX_PORT + 1):
        try:
            server = HTTPServer((session.HOST, p), _BlenderHTTPHandler)
            _st._http_server = server
            _st.server_port = p
            _st._http_thread = threading.Thread(
                target=server.serve_forever, daemon=True)
            _st._http_thread.start()
            _log(f"Blender server on :{p}")
            return p
        except OSError:
            continue
    _err("Could not start Blender HTTP server (ports 8791-8795 busy)")
    return 0


def _stop_blender_server():
    """Stop Blender's HTTP server."""
    if _st._http_server:
        _st._http_server.shutdown()
        if _st._http_thread:
            _st._http_thread.join(timeout=3)
        _st._http_server.server_close()
        _st._http_server = None
        _st._http_thread = None
        _st.server_port = 0
        _log("Blender server stopped")


def _apply_incoming_transforms(params):
    """Apply transforms received from UEFN. Reverse mapping: UE → Blender."""
    objects_data = params.get("objects")
    if not isinstance(objects_data, list) or not _connected_file():
        return

    _st.receiving = True
    try:
        all_objs = {}
        for obj in bpy.context.scene.objects:
            guid = obj.get("bb_guid")
            if obj.type == "MESH" and isinstance(guid, str) and guid in _st.synced_guids:
                all_objs[guid] = obj if guid not in all_objs else None
        updated = 0

        for od in objects_data:
            if not isinstance(od, dict) or not isinstance(od.get("guid"), str):
                continue
            obj = all_objs.get(od["guid"])
            if obj is None:
                continue

            ue_loc = od.get("location")
            ue_rot = od.get("rotation")  # [Pitch, Yaw, Roll]
            ue_scale = od.get("scale")
            if not all(_valid_vector(v) for v in (ue_loc, ue_rot, ue_scale)):
                continue

            # Reverse UE → Blender into a WORLD matrix (single-source convention).
            # matrix_world handles parenting (parent-inverse) and any rotation_mode
            # (euler / quaternion / axis-angle) correctly — unlike writing local fields.
            loc = mathutils.Vector(coords.loc_ue_to_bl(*ue_loc))
            eul = mathutils.Euler(coords.rot_ue_to_bl(*ue_rot), "XYZ")
            scl = mathutils.Vector((ue_scale[0], ue_scale[1], ue_scale[2]))
            obj.matrix_world = mathutils.Matrix.LocRotScale(loc, eul, scl)
            if obj.name in _st.last_obj_hashes:
                _st.last_obj_hashes[obj.name]["trans"] = _obj_transform_hash(obj)
            updated += 1

        if updated:
            _log(f"Received {updated} transform(s) from UEFN")
            _st.status = f"UEFN → Blender: {updated} updated"

    finally:
        _st.receiving = False


def _valid_vector(vector):
    return (isinstance(vector, list) and len(vector) == 3
            and all(type(n) in (int, float) and -1e12 <= n <= 1e12 for n in vector))


def _process_incoming():
    """Process incoming commands from UEFN (main thread timer)."""
    try:
        while not _incoming_queue.empty():
            command, message = _incoming_queue.get_nowait()
            generation, *data = message
            if generation != _st.generation:
                continue
            _st.sending = False
            if command == "send_failed":
                _st.status = "Send failed"
                _err(data[0])
                continue
            label, result, snapshot, objects = data
            try:
                _remember_actors(objects, result, "actors")
            except RuntimeError as exc:
                _st.status = "Send incomplete"
                _err(str(exc))
                continue
            actors = result.get("actors", len(objects))
            _st.status = f"{label}: {actors}"
            _st.last_error = ""
            _st.send_count += 1
            if snapshot is not None:
                _st.last_obj_hashes.update(snapshot)
        for _ in range(4):
            try:
                generation, params = _transform_queue.get_nowait()
            except _queue_mod.Empty:
                break
            if generation == _st.generation:
                _apply_incoming_transforms(params)
        if _st.pending_save and not _st.sending:
            _st.pending_save = False
            if _st.live_active:
                _on_save()
    except Exception as e:
        _err(f"Incoming process error: {e}")
    # Redraw panels while an async send is in flight so its status updates show live (this runs
    # on the main thread — safe to touch bpy, unlike the send worker).
    if _st.sending:
        try:
            for win in bpy.context.window_manager.windows:
                for area in win.screen.areas:
                    if area.type == "VIEW_3D":
                        area.tag_redraw()
        except Exception:
            pass
    return 0.1  # re-run every 100ms


# ============================================================
# SCENE HASHING
# ============================================================


def _obj_geometry_hash(obj):
    """Hash a single object's mesh geometry (vertices + polygons)."""
    h = hashlib.md5()
    try:
        depsgraph = bpy.context.evaluated_depsgraph_get()
        eo = obj.evaluated_get(depsgraph)
        mesh = eo.to_mesh()
        vc = len(mesh.vertices)
        h.update(struct.pack("ii", vc, len(mesh.polygons)))
        if vc <= 5000:
            for v in mesh.vertices:
                h.update(struct.pack("3f", v.co.x, v.co.y, v.co.z))
        else:
            step = max(1, vc // 500)
            for i in range(0, vc, step):
                h.update(struct.pack("3f",
                         mesh.vertices[i].co.x,
                         mesh.vertices[i].co.y,
                         mesh.vertices[i].co.z))
        eo.to_mesh_clear()
    except Exception:
        pass
    return h.hexdigest()


def _obj_material_hash(obj):
    """Hash a single object's material properties (excludes textures)."""
    h = hashlib.md5()
    if hasattr(obj.data, "materials"):
        for mat in obj.data.materials:
            if not mat:
                continue
            h.update(struct.pack("4f", *mat.diffuse_color))
            h.update(struct.pack("2f", mat.metallic, mat.roughness))
            if mat.use_nodes and mat.node_tree:
                for node in mat.node_tree.nodes:
                    if node.bl_idname == "ShaderNodeBsdfPrincipled":
                        for inp in node.inputs:
                            if hasattr(inp, "default_value"):
                                try:
                                    if hasattr(inp.default_value, "__iter__"):
                                        for val in inp.default_value:
                                            h.update(struct.pack("f", round(float(val), 4)))
                                    else:
                                        h.update(struct.pack("f", round(float(inp.default_value), 4)))
                                except Exception:
                                    pass
    return h.hexdigest()


def _obj_texture_hash(obj):
    """Hash a single object's texture image references."""
    h = hashlib.md5()
    if hasattr(obj.data, "materials"):
        for mat in obj.data.materials:
            if not mat or not mat.use_nodes or not mat.node_tree:
                continue
            for node in mat.node_tree.nodes:
                if node.type == "TEX_IMAGE" and node.image:
                    h.update(node.image.name.encode())
                    if node.image.filepath:
                        h.update(node.image.filepath.encode())
    return h.hexdigest()


def _obj_transform_hash(obj):
    """Hash a single object's WORLD transform (location, rotation, scale) + collection.
    Reads matrix_world (the SAME source as _obj_data) so the diff detects exactly what gets
    sent — including parent-driven changes. All three are actor-applied (data-driven), so any
    transform change is a FAST update — no re-export."""
    h = hashlib.md5()
    loc, rot_q, sc = obj.matrix_world.decompose()
    rot = rot_q.to_euler("XYZ")
    h.update(struct.pack("9f", loc.x, loc.y, loc.z, rot.x, rot.y, rot.z, sc.x, sc.y, sc.z))
    h.update(_get_collection_path(obj).encode())
    return h.hexdigest()


def _snapshot_all():
    """Snapshot all mesh objects' hashes for diff comparison."""
    # Flush the depsgraph so matrix_world reflects edits made in the N-panel/viewport.
    # Without this, matrix_world stays STALE until Blender flushes on its own (a save forces
    # it) — which is why a rotate wasn't picked up by Send Changes until the user saved first.
    bpy.context.view_layer.update()
    snap = {}
    for obj in bpy.context.scene.objects:
        if obj.type != "MESH":
            continue
        snap[obj.name] = {
            "geo": _obj_geometry_hash(obj),
            "mat": _obj_material_hash(obj),
            "tex": _obj_texture_hash(obj),
            "trans": _obj_transform_hash(obj),
        }
    return snap


def _compute_diff(old_snap, new_snap):
    """Compare two snapshots and categorize changes."""
    old_names = set(old_snap.keys())
    new_names = set(new_snap.keys())

    diff = {
        "added": list(new_names - old_names),
        "removed": list(old_names - new_names),
        "geo_changed": [],
        "mat_changed": [],
        "tex_changed": [],
        "trans_changed": [],
    }

    for name in old_names & new_names:
        old, new = old_snap[name], new_snap[name]
        for category in ("geo", "mat", "tex", "trans"):
            if old.get(category) != new[category]:
                diff[f"{category}_changed"].append(name)

    return diff

# ============================================================
# EXCHANGE FOLDER + EXPORT
# ============================================================


def _safe_filename(name):
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", name).strip("_")[:100] or "Asset"
    if safe.upper() in {"CON", "PRN", "AUX", "NUL"} or re.fullmatch(r"(?:COM|LPT)[1-9]", safe.upper()):
        return f"Asset_{safe}"
    return safe


def _export_textures(objects, stage):
    """Copy textures used by the outgoing meshes without modifying their images."""
    tex_dir = stage / "textures"
    tex_dir.mkdir()
    tex_paths = {}
    extensions = {"BMP": "bmp", "JPEG": "jpg", "PNG": "png", "TARGA": "tga",
                  "TARGA_RAW": "tga", "TIFF": "tiff", "OPEN_EXR": "exr", "HDR": "hdr"}
    for obj in objects:
        for slot in obj.material_slots:
            mat = slot.material
            if not mat or not mat.use_nodes or not mat.node_tree:
                continue
            for node in mat.node_tree.nodes:
                img = node.image if node.type == "TEX_IMAGE" else None
                if not img or img.name in tex_paths:
                    continue
                ext = extensions.get(img.file_format)
                if not ext:
                    _log(f"Skipping texture {img.name} with unsupported format {img.file_format}")
                    continue
                prefix = hashlib.sha256(img.name.encode("utf-8")).hexdigest()[:12]
                safe_name = _safe_filename(os.path.splitext(img.name)[0])
                filepath = tex_dir / f"T_{prefix}_{safe_name}.{ext}"
                try:
                    img.save(filepath=str(filepath), save_copy=True)
                except TypeError:
                    copy = img.copy()
                    try:
                        copy.filepath_raw = str(filepath)
                        copy.save()
                    finally:
                        bpy.data.images.remove(copy)
                if filepath.is_file():
                    tex_paths[img.name] = str(filepath)
    return tex_paths


def _export_material_info(objects, tex_paths, stage):
    """Write one material record per Blender material used by the outgoing meshes."""
    materials = {}
    for obj in objects:
        for slot in obj.material_slots:
            mat = slot.material
            if not mat or mat.name in materials:
                continue
            info = {
                "name": mat.name,
                "object_name": obj.name,
                "diffuse_color": list(mat.diffuse_color),
                "metallic": mat.metallic,
                "roughness": mat.roughness,
                "specular": mat.specular_intensity,
                "textures": {},
            }
            if mat.use_nodes and mat.node_tree:
                for node in mat.node_tree.nodes:
                    if node.type == "TEX_IMAGE" and node.image and node.image.name in tex_paths:
                        for out in node.outputs:
                            for link in out.links:
                                info["textures"][link.to_socket.name.lower()] = {
                                    "name": node.image.name,
                                    "path": tex_paths[node.image.name],
                                    "colorspace": node.image.colorspace_settings.name,
                                }
                    if node.bl_idname == "ShaderNodeBsdfPrincipled":
                        for socket in node.inputs:
                            key = socket.name.lower().replace(" ", "_")
                            if key not in ("base_color", "metallic", "roughness", "specular", "specular_ior_level"):
                                continue
                            value = socket.default_value
                            info[key] = float(value) if isinstance(value, (float, int)) else list(value)
            materials[mat.name] = info
    with (stage / "materials.json").open("x", encoding="utf-8") as manifest:
        json.dump(list(materials.values()), manifest, allow_nan=False)


def _export_fbx(stage):
    """Export the selected meshes to the transfer directory."""
    filepath = str(stage / "scene.fbx")

    if not any(obj.select_get() for obj in bpy.context.scene.objects):
        return None

    # DATA-DRIVEN export: neutralize the WORLD matrix (parent + local) so the FBX carries the
    # PURE mesh data — no location, no rotation, no scale, no parent influence. The UEFN actor
    # supplies the full world transform (location + rotation + world-scale). UEFN reads FBX
    # numbers as METERS, so global_scale=1 makes m->cm automatic; the actor's world-scale then
    # gives the correct size in ANY case (parented, unparented, scaled). Zeroing only the LOCAL
    # transform was wrong: a parent's scale leaked into the geometry (100x gigantic once the
    # parent was cleared and that scale folded into local).
    selected_meshes = [o for o in bpy.context.scene.objects
                       if o.select_get() and o.type == "MESH"]
    # Instancing (F-44): export only ONE object per unique mesh datablock (the representative);
    # linked duplicates reference it via their "mesh" field and become extra actors in UEFN.
    rep_names = set(_mesh_rep_map(selected_meshes).values())
    for o in selected_meshes:
        if o.name not in rep_names:
            o.select_set(False)
    selected_meshes = [o for o in selected_meshes if o.name in rep_names]
    # Keep a SELECTED object active — deselecting non-reps can orphan the active object, which
    # makes the FBX exporter produce an empty file on some selections.
    if selected_meshes:
        bpy.context.view_layer.objects.active = selected_meshes[0]
    # LOD (combined export): rename each <base>_LOD<N> member to LOD<xx>_<base> so UEFN/Interchange
    # builds ONE StaticMesh <base> with LOD levels. selected_meshes is already instance-deduped,
    # so a forest collapses to one LOD0 + one LOD1 rep here. Restored after export.
    lod_member = {}   # original obj name -> (base, level)
    _lb = _lod_bases(selected_meshes)
    for o in selected_meshes:
        p = _lod_parse(o.name)
        if p and p[0] in _lb:
            lod_member[o.name] = (p[0], p[1])

    saved = {}
    name_saved = []   # (obj, original_name) for LOD-renamed members
    for o in selected_meshes:
        saved[o.name] = (o.location.copy(), o.rotation_euler.copy(),
                         o.rotation_quaternion.copy(), o.scale.copy(), o.rotation_mode)
        o.matrix_world = mathutils.Matrix.Identity(4)
        lm = lod_member.get(o.name)
        if lm:
            name_saved.append((o, o.name))
            o.name = f"LOD{lm[1]:02d}_{lm[0]}"
    bpy.context.view_layer.update()

    def _restore():
        for o, orig in name_saved:   # LOD names first, so saved[] keys (original names) match
            o.name = orig
        for o in selected_meshes:
            if o.name in saved:
                loc, eul, quat, scl, mode = saved[o.name]
                o.location, o.rotation_euler, o.rotation_quaternion, o.scale = loc, eul, quat, scl
                o.rotation_mode = mode

    try:
        for area in bpy.context.screen.areas:
            if area.type == "VIEW_3D":
                with bpy.context.temp_override(area=area):
                    bpy.ops.export_scene.fbx(
                        filepath=filepath,
                        use_selection=True,
                        global_scale=1.0,             # UEFN reads FBX as meters -> m->cm automatic
                        apply_unit_scale=False,
                        apply_scale_options="FBX_SCALE_NONE",
                        axis_forward="X", axis_up="Z",  # bake Z-up INTO the FBX (stands upright)
                        use_space_transform=True,
                        bake_space_transform=False,
                        mesh_smooth_type="FACE",
                        use_mesh_modifiers=True,
                        object_types={"MESH"},
                        bake_anim=False,
                        add_leaf_bones=False,
                        # Mesh-only FBX: textures travel via the separate texture export +
                        # materials.json, and UEFN no longer imports embedded materials — so
                        # copying/embedding them here just bloats the file and slows the import.
                        path_mode="STRIP",
                        embed_textures=False,
                    )
                return filepath if Path(filepath).is_file() else None
    finally:
        _restore()

    return None


def _export_fbx_objects(obj_names, stage):
    """Export individual FBX files per object. Returns {obj_name: fbx_path}."""
    results = {}
    area_3d = None
    for area in bpy.context.screen.areas:
        if area.type == "VIEW_3D":
            area_3d = area
            break
    if not area_3d:
        return results

    all_objs = {o.name: o for o in bpy.context.scene.objects if o.type == "MESH"}

    # Instancing (F-44): export one FBX per unique mesh datablock (the representative); the other
    # sharers reference it via their "mesh" field in _obj_data and become extra actors in UEFN.
    sel_objs = [all_objs[n] for n in obj_names if n in all_objs]
    rep_names = sorted(set(_mesh_rep_map(sel_objs).values()))

    for name in rep_names:
        obj = all_objs.get(name)
        if not obj:
            continue

        object_dir = stage / uuid.uuid4().hex
        object_dir.mkdir()
        filepath = str(object_dir / f"{_safe_filename(name)}.fbx")

        # Select only this object
        bpy.ops.object.select_all(action="DESELECT")
        obj.select_set(True)
        bpy.context.view_layer.objects.active = obj

        # DATA-DRIVEN: neutralize the WORLD matrix (parent + local) so the FBX carries the PURE
        # mesh data; the actor carries the full world location/rotation/world-scale. UEFN reads
        # FBX as METERS, so global_scale=1 makes m->cm automatic and the actor's world-scale
        # sizes it correctly in any case (parented, unparented, scaled). See _export_fbx.
        saved = (obj.location.copy(), obj.rotation_euler.copy(),
                 obj.rotation_quaternion.copy(), obj.scale.copy(), obj.rotation_mode)
        obj.matrix_world = mathutils.Matrix.Identity(4)
        bpy.context.view_layer.update()

        try:
            with bpy.context.temp_override(area=area_3d):
                bpy.ops.export_scene.fbx(
                    filepath=filepath,
                    use_selection=True,
                    global_scale=1.0,             # UEFN reads FBX as meters -> m->cm automatic
                    apply_unit_scale=False,
                    apply_scale_options="FBX_SCALE_NONE",
                    axis_forward="X", axis_up="Z",  # bake Z-up INTO the FBX (stands upright)
                    use_space_transform=True,
                    bake_space_transform=False,
                    mesh_smooth_type="FACE",
                    use_mesh_modifiers=True,
                    object_types={"MESH"},
                    bake_anim=False,
                    add_leaf_bones=False,
                    # Mesh-only FBX (see _export_fbx) — textures go via the separate export.
                    path_mode="STRIP",
                    embed_textures=False,
                )
            if Path(filepath).is_file():
                results[name] = filepath
        except Exception as e:
            _err(f"FBX export failed for {name}: {e}")
        finally:
            obj.location, obj.rotation_euler, obj.rotation_quaternion, obj.scale, obj.rotation_mode = saved
            bpy.context.view_layer.update()

    return results


def _collect_material_data(objects):
    """Extract material property data for the given objects."""
    materials = []
    for o in objects:
        if not hasattr(o.data, "materials"):
            continue
        for mat in o.data.materials:
            if not mat:
                continue
            mat_info = {
                "object_name": o.name,
                "name": mat.name,
                "diffuse_color": list(mat.diffuse_color),
                "metallic": mat.metallic,
                "roughness": mat.roughness,
            }
            if mat.use_nodes and mat.node_tree:
                for node in mat.node_tree.nodes:
                    if node.bl_idname == "ShaderNodeBsdfPrincipled":
                        for inp in node.inputs:
                            if hasattr(inp, "default_value"):
                                try:
                                    if inp.name == "Base Color":
                                        mat_info["base_color"] = list(inp.default_value)
                                    elif inp.name == "Metallic":
                                        mat_info["metallic"] = float(inp.default_value)
                                    elif inp.name == "Roughness":
                                        mat_info["roughness"] = float(inp.default_value)
                                    elif inp.name == "Specular IOR Level":
                                        mat_info["specular"] = float(inp.default_value)
                                except Exception:
                                    pass
            materials.append(mat_info)
    return materials


# ============================================================
# MESH HELPERS
# ============================================================


def _get_mesh_children(obj):
    """Recursively get all mesh children."""
    result = []
    for child in obj.children:
        if child.type == "MESH":
            result.append(child)
        result.extend(_get_mesh_children(child))
    return result


def _collect_selected_meshes():
    """Get selected mesh objects including children."""
    meshes = set()
    for obj in bpy.context.selected_objects:
        if obj.type == "MESH":
            meshes.add(obj)
        meshes.update(_get_mesh_children(obj))
    return list(meshes)


def _get_collection_path(obj):
    """Get the deepest collection path for an object, excluding Scene Collection.
    Returns e.g. 'Buildings/Residential' or '' if only in Scene Collection root.
    """
    def _find(col, path):
        results = []
        for child in col.children:
            child_path = f"{path}/{child.name}" if path else child.name
            if obj.name in child.objects:
                results.append(child_path)
            results.extend(_find(child, child_path))
        return results

    paths = _find(bpy.context.scene.collection, "")
    if not paths:
        return ""
    return max(paths, key=lambda p: p.count("/"))


def _dedup_guids():
    """Ensure every mesh object has a UNIQUE bb_guid. Blender COPIES custom properties on
    duplicate (Shift+D / Alt+D), so duplicates inherit the source's bb_guid and would all
    collapse onto the SAME UEFN actor (the bridge matches actors by GUID). The first holder
    (by name) keeps the guid; every later collider gets a fresh one (persisted = stable)."""
    seen = set()
    for o in sorted((ob for ob in bpy.context.scene.objects if ob.type == "MESH"),
                    key=lambda x: x.name):
        g = o.get("bb_guid")
        if g is None:
            continue
        if not isinstance(g, str) or not session.GUID_PATTERN.fullmatch(g) or g in seen:
            g = uuid.uuid4().hex
            o["bb_guid"] = g
        seen.add(g)


def _lod_parse(name):
    """(base, level, inst) for a LOD-named object, else None. Handles Blender's '.NNN' duplicate
    suffix so a LOD'd mesh AND its instances group correctly:
      'Tree_1_1_LOD0'      -> ('Tree_1_1', 0, '')        # original, level 0
      'Tree_1_1_LOD0.001'  -> ('Tree_1_1', 0, '.001')    # instance .001, level 0
      'Tree_1_1_LOD1.001'  -> ('Tree_1_1', 1, '.001')    # instance .001, level 1
    So a forest of LOD'd trees = one base, N instances, each with its LOD levels."""
    inst = ""
    core = name
    head, _, tail = name.rpartition(".")
    if head and tail.isdigit():
        core, inst = head, "." + tail
    low = core.lower()
    idx = low.rfind("_lod")
    if idx < 0:
        return None
    suf = core[idx + 4:]
    return (core[:idx], int(suf), inst) if suf.isdigit() else None


def _lod_bases(objects):
    """Set of base names that form a real LOD group (2+ distinct levels across all instances)."""
    levels = {}
    for o in objects:
        if o.type != "MESH":
            continue
        p = _lod_parse(o.name)
        if p:
            levels.setdefault(p[0], set()).add(p[1])
    return {b for b, lv in levels.items() if len(lv) >= 2}


def _mesh_rep_map(objects):
    """Map each mesh datablock name -> its REPRESENTATIVE object name (min name) among `objects`.
    Objects sharing a datablock (linked duplicates) resolve to one representative, so they export
    ONE mesh and spawn N actors in UEFN (instancing — F-44)."""
    rep = {}
    for o in objects:
        if o.type != "MESH" or not o.data:
            continue
        key = o.data.name
        if key not in rep or o.name < rep[key]:
            rep[key] = o.name
    return rep


def _obj_data(objects):
    """Build object data list with Blender→UE coordinate conversion.

    Uses matrix_world.decompose() to get the TRUE world transform,
    handling parents, constraints, and delta transforms correctly.

    Blender: X=right, Y=forward, Z=up (right-handed, meters)
    UE/UEFN: X=forward, Y=right, Z=up (left-handed, centimeters)

    The FBX pipeline handles axis reorientation for mesh vertices.
    World coordinates only need the handedness flip (negate Y).
    The convention lives in coords.py (single source, unit-tested).
    """
    # Flush the depsgraph so matrix_world is current (see _snapshot_all) — guarantees we send
    # the just-edited transform, not a stale one, on the send_selected/send_scene paths too.
    bpy.context.view_layer.update()
    _dedup_guids()                  # duplicates (Alt+D/Shift+D) inherit bb_guid -> give fresh ones
    reps = _mesh_rep_map(objects)   # instancing (F-44): shared datablock -> one representative

    # LOD + instance: group LOD members by (base, instance). Each instance becomes ONE actor
    # entry (name base[+.NNN], mesh=base, its LOD0 transform) and they ALL share SM_<base> —
    # which the combined export builds (instance rep-dedup collapses the dups, LOD rename groups
    # the levels). So a forest of LOD'd trees = N actors, ONE LOD'd StaticMesh.
    lod_bases = _lod_bases(objects)
    inst_levels = {}        # (base, inst) -> {level: obj}
    for o in objects:
        if o.type == "MESH":
            p = _lod_parse(o.name)
            if p and p[0] in lod_bases:
                inst_levels.setdefault((p[0], p[2]), {})[p[1]] = o
    lod_primary = {}        # primary (LOD0) obj name -> (base, inst)
    lod_skip = set()        # every LOD-member obj name
    for (base, inst), levels in inst_levels.items():
        lod_skip.update(o.name for o in levels.values())
        lod_primary[levels[min(levels)].name] = (base, inst)

    result = []
    for o in objects:
        if o.name in lod_skip and o.name not in lod_primary:
            continue  # non-primary LOD level — folded into its instance's LOD chain
        mw = o.matrix_world.copy()
        loc, rot_q, sc = mw.decompose()
        rot = rot_q.to_euler('XYZ')

        # Stable identity that survives rename/duplicate (B3): a GUID stored as a custom
        # property on the object, mirrored to a tag on the UEFN actor.
        guid = o.get("bb_guid")
        if not guid:
            guid = uuid.uuid4().hex
            o["bb_guid"] = guid

        if o.name in lod_primary:           # LOD instance → name base[+.NNN], mesh = base
            base, inst = lod_primary[o.name]
            ename = base + inst
            emesh = base
        else:
            ename = o.name
            emesh = reps.get(o.data.name, o.name) if o.data else o.name

        result.append({
            "name": ename,
            "guid": guid,
            # Representative mesh name for this object's datablock — instances share it (F-44).
            "mesh": emesh,
            # Ordered material slots -> Blender material name (UEFN assigns MI_ per slot; shared
            # material name => one shared MI_, deduped). "" for an empty slot.
            "materials": [(s.material.name if s.material else "") for s in o.material_slots],
            "collection": _get_collection_path(o),
            "location": coords.loc_bl_to_ue(loc.x, loc.y, loc.z),
            # DATA-DRIVEN: mesh is exported clean (no baked rotation), the actor carries the
            # full world rotation. This conversion is the CALIBRATION TARGET (coords.py).
            "rotation": coords.rot_bl_to_ue(rot.x, rot.y, rot.z),
            "scale": [sc.x, sc.y, sc.z],
        })
    return result

# ============================================================
# SEND COMMANDS
# ============================================================


def _blend_project_name():
    """Project folder name = the .blend filename (no extension). Auto-identifies the Blender
    project — no typing — and keeps each .blend's assets in its own UEFN subfolder. '' if unsaved."""
    path = bpy.data.filepath
    return os.path.splitext(os.path.basename(path))[0] if path else ""


def _current_blend_path():
    return Path(bpy.data.filepath).resolve() if bpy.data.filepath else None


def _connected_file():
    if not _st.connected:
        return False
    if _current_blend_path() != _st.blend_path:
        do_disconnect(notify=False)
        _st.status = "The Blender file changed. Reconnect to sync it."
        return False
    return True


def do_connect():
    """Connect to UEFN bridge."""
    # The project IS the .blend file — its filename names the UEFN folder, fully automatic
    # (no field to type). A STABLE name is required from the first sync, so block if unsaved
    # (connecting then saving later would move the folder and orphan what's already synced).
    pname = _blend_project_name()
    if not pname:
        _st.status = "Save the .blend first"
        _err("Save the .blend (Ctrl+S) before connecting — the filename names the UEFN "
             "project folder, so your assets stay in one stable place.")
        return False

    _st.last_error = ""
    _st.status = "Connecting..."

    try:
        _st.token, _st.exchange = session.read_session()
        if not _alive():
            raise ConnectionError("Cannot reach the authenticated UEFN bridge. Start its editor script first.")
        if not _start_blender_server():
            raise ConnectionError("No local Blender bridge port is available.")
        r = _send("blender_connect", {
            "blender_version": bpy.app.version_string,
            "addon_version": ADDON_VERSION,
            "project_name": pname,
            "server_port": _st.server_port,
        })
        _st.blend_path = _current_blend_path()
        _st.generation += 1
        _st.connected = True
        _st.project_path = r.get("project_path", "")
        _st.project_name = pname
        guids = r.get("guids", [])
        if isinstance(guids, list):
            _st.synced_guids = frozenset(
                guid for guid in guids if isinstance(guid, str) and session.GUID_PATTERN.fullmatch(guid))
        _st.status = "Connected"
        _log(f"Connected! Project: {_st.project_path} / {pname}")

        # Start incoming queue timer
        if not bpy.app.timers.is_registered(_process_incoming):
            bpy.app.timers.register(_process_incoming, first_interval=0.5,
                                    persistent=True)

        return True
    except Exception as e:
        do_disconnect(notify=False)
        _st.status = f"Failed: {e}"
        _err(f"Connect failed: {e}")
        return False


def do_disconnect(notify=True):
    """Disconnect from UEFN bridge."""
    if _st.live_active:
        do_stop_live(notify=notify)
    if _st.connected and notify:
        try:
            _send("blender_disconnect", timeout=3)
        except Exception:
            pass
    _st.connected = False
    _st.generation += 1
    _st.status = "Disconnected"
    _st.project_path = ""
    _st.project_name = ""
    _st.blend_path = None
    _st.token = ""
    _st.exchange = None
    _st.sending = False
    _st.pending_save = False
    _st.synced_guids = frozenset()
    _st.last_obj_hashes = {}
    # Stop incoming timer
    if bpy.app.timers.is_registered(_process_incoming):
        bpy.app.timers.unregister(_process_incoming)
    _stop_blender_server()
    while not _transform_queue.empty():
        try:
            _transform_queue.get_nowait()
        except _queue_mod.Empty:
            break
    _log("Disconnected")


@contextmanager
def _preserve_selection():
    view_objects = bpy.context.view_layer.objects
    active = view_objects.active
    selected = list(bpy.context.selected_objects)
    try:
        yield
    finally:
        for obj in bpy.context.selected_objects:
            obj.select_set(False)
        for obj in selected:
            obj.select_set(True)
        view_objects.active = active


def _export_selection(objects, stage):
    """Export just these objects and restore the user's selection afterward."""
    with _preserve_selection():
        for obj in bpy.context.selected_objects:
            obj.select_set(False)
        for obj in objects:
            obj.select_set(True)
        bpy.context.view_layer.objects.active = objects[0]
        return _export_fbx(stage)


def _send_meshes(objects, selected_only):
    if not _connected_file() or _st.sending:
        return False
    if bpy.context.mode != "OBJECT":
        _err("Switch to Object Mode before sending meshes.")
        return False
    if not objects:
        _err("There are no meshes to export.")
        return False

    _st.last_error = ""
    try:
        _dedup_guids()
        existing = [obj for obj in objects if _is_synced(obj)]
        fresh = [obj for obj in objects if not _is_synced(obj)]
        snapshot = _snapshot_all()
        if existing:
            payload = _sync_geometry(existing, "update_objects", "updated")
            _remember_actors(payload, _send("update_transforms", {"objects": payload}), "updated")
            _save_snapshot(snapshot, [obj.name for obj in existing], ("geo", "trans"))
        if not fresh:
            _st.status = f"Updated {len(existing)} meshes"
            _st.send_count += 1
            return True

        stage = session.make_stage(_st.exchange)
        payload = _obj_data(fresh)
        _export_material_info(fresh, _export_textures(fresh, stage), stage)
        fbx_path = _export_selection(fresh, stage)
        if not fbx_path:
            raise RuntimeError("Blender did not produce an FBX file.")
        snapshot = {obj.name: snapshot[obj.name] for obj in fresh}
        params = {
            "fbx_path": fbx_path,
            "scene_name": bpy.context.scene.name,
            "object_count": len(payload),
            "objects": payload,
            "exchange_folder": str(stage),
            "selected_only": selected_only,
            "combine_meshes": bpy.context.scene.uefn_bridge.combine_meshes,
        }
        _send_async("import_scene", params, "Sent selected" if selected_only else "Sent", snapshot)
        return True
    except Exception as exc:
        _st.status = "Send failed"
        _err(str(exc))
        return False


def do_send_scene():
    objects = [obj for obj in bpy.context.view_layer.objects if obj.type == "MESH"]
    return _send_meshes(objects, selected_only=False)


def do_send_selected():
    return _send_meshes(_collect_selected_meshes(), selected_only=True)


def _save_snapshot(new_snapshot, names, categories):
    for name in names:
        old = _st.last_obj_hashes.setdefault(name, {})
        for category in categories:
            old[category] = new_snapshot[name][category]


def _remember_actors(payload, result, count_key):
    acknowledged = result.get("guids")
    if isinstance(acknowledged, list):
        expected = {obj["guid"] for obj in payload}
        guids = frozenset(guid for guid in acknowledged if isinstance(guid, str) and guid in expected)
    elif result.get(count_key) == len(payload):
        guids = frozenset(obj["guid"] for obj in payload)
    else:
        raise RuntimeError("UEFN reported a partial update. Save again to retry.")
    _st.synced_guids |= guids
    if len(guids) != len(payload):
        raise RuntimeError("UEFN reported a partial update. Save again to retry.")


def _is_synced(obj):
    guid = obj.get("bb_guid")
    return isinstance(guid, str) and guid in _st.synced_guids


def _sync_geometry(targets, command, count_key):
    stage = session.make_stage(_st.exchange)
    payload = _obj_data(targets)
    with _preserve_selection():
        fbx_paths = _export_fbx_objects([obj.name for obj in targets], stage)
    if len(fbx_paths) != len(_mesh_rep_map(targets)):
        raise RuntimeError("Blender could not export every changed mesh.")
    if command == "add_objects":
        _export_material_info(targets, _export_textures(targets, stage), stage)
    result = _send(command, {
        "fbx_paths": fbx_paths,
        "objects": payload,
        "scene_name": bpy.context.scene.name,
        "exchange_folder": str(stage),
    }, timeout=600)
    _remember_actors(payload, result, count_key)
    return payload


def do_send_changes():
    """Update changed geometry, textures, materials, and transforms."""
    if not _connected_file():
        return False
    if _st.sending:
        _st.status = "Busy. A send is already in progress."
        return False
    if bpy.context.mode != "OBJECT":
        _err("Switch to Object Mode before sending changes.")
        return False

    _dedup_guids()
    new_snapshot = _snapshot_all()
    diff = _compute_diff(_st.last_obj_hashes, new_snapshot)
    scene_name = bpy.context.scene.name
    objects = {obj.name: obj for obj in bpy.context.view_layer.objects if obj.type == "MESH"}
    new_names = set(diff["added"])
    geometry = new_names | set(diff["geo_changed"])
    other_changes = set(diff["mat_changed"]) | set(diff["tex_changed"]) | set(diff["trans_changed"])
    added = {name for name in (geometry | other_changes) & objects.keys()
             if not _is_synced(objects[name])}
    geometry -= added
    changed = False
    try:
        for candidates, command, count_key in (
            (added, "add_objects", "added"),
            (geometry, "update_objects", "updated"),
        ):
            names = sorted(candidates & objects.keys())
            if not names:
                continue
            targets = [objects[name] for name in names]
            _sync_geometry(targets, command, count_key)
            categories = ("geo", "mat", "tex", "trans") if command == "add_objects" else ("geo",)
            _save_snapshot(new_snapshot, names, categories)
            changed = True

        names = [name for name in diff["tex_changed"] if name in objects and name not in added]
        if names:
            targets = [objects[name] for name in names]
            stage = session.make_stage(_st.exchange)
            _export_material_info(targets, _export_textures(targets, stage), stage)
            _send("update_textures", {
                "objects": _obj_data(targets),
                "object_names": names,
                "exchange_folder": str(stage),
                "scene_name": scene_name,
            }, timeout=600)
            _save_snapshot(new_snapshot, names, ("tex",))
            changed = True

        names = [name for name in diff["mat_changed"] if name in objects and name not in added]
        if names:
            materials = _collect_material_data([objects[name] for name in names])
            if materials:
                _send("update_materials", {
                    "materials": materials,
                    "objects": _obj_data([objects[name] for name in names]),
                })
            _save_snapshot(new_snapshot, names, ("mat",))
            changed = True

        names = [name for name in diff["trans_changed"] if name in objects and name not in added]
        if names:
            payload = _obj_data([objects[name] for name in names])
            result = _send("update_transforms", {"objects": payload})
            _remember_actors(payload, result, "updated")
            _save_snapshot(new_snapshot, names, ("trans",))
            changed = True
    except Exception as exc:
        _st.status = "Sync failed"
        _err(str(exc))
        return False

    for name in diff["removed"]:
        _st.last_obj_hashes.pop(name, None)
    if changed:
        _st.status = "Changes sent to UEFN"
        _st.last_error = ""
        _st.send_count += 1
    elif diff["removed"]:
        _st.status = "Removed Blender objects were kept in UEFN"
    else:
        _st.status = "No changes detected"
    return changed


# ============================================================
# LIVE SYNC (triggers on Ctrl+S)
# ============================================================


@persistent
def _on_load(*_args):
    if _st.connected:
        do_disconnect(notify=False)


@persistent
def _on_save(*_args):
    """Called after every Ctrl+S — syncs to UEFN if live mode is active."""
    if not _st.live_active or not _connected_file():
        return
    if bpy.context.scene.name != _st.live_scene:
        _st.status = "Return to the scene where live sync started."
        return
    if _st.sending:
        _st.pending_save = True
        return
    # Don't sync back when applying inbound changes from UEFN
    if _st.receiving:
        _log("Skipping outbound sync — applying inbound changes")
        return
    # Block sync in Edit Mode — FBX export requires Object Mode
    if bpy.context.mode != "OBJECT":
        _log("Skipping sync — not in Object Mode (exit Edit Mode first)")
        return
    try:
        _log("Save detected — syncing to UEFN...")
        do_send_changes()
    except Exception as e:
        _err(f"Save sync error: {e}")


def do_start_live():
    """Start live sync — auto-syncs on every Ctrl+S."""
    if _st.live_active or not _connected_file():
        return False
    try:
        _send("set_live_sync", {"active": True, "blender_port": _st.server_port}, timeout=5)
    except Exception as e:
        _err(f"Could not start live sync: {e}")
        return False
    _st.live_active = True
    _st.live_scene = bpy.context.scene.name
    for name, value in _snapshot_all().items():
        _st.last_obj_hashes.setdefault(name, value)
    if _on_save not in bpy.app.handlers.save_post:
        bpy.app.handlers.save_post.append(_on_save)
    _st.status = "Live: Blender⇄UEFN"
    _log("Live sync started (Blender→UEFN on save; UEFN→Blender live)")
    return True


def do_stop_live(notify=True):
    """Stop live sync."""
    _st.live_active = False
    _st.live_scene = None
    _st.pending_save = False
    if _on_save in bpy.app.handlers.save_post:
        bpy.app.handlers.save_post.remove(_on_save)
    if notify:
        try:
            _send("set_live_sync", {"active": False}, timeout=3)
        except Exception:
            pass
    _st.status = "Connected"
    _log("Live sync stopped")

# ============================================================
# OPERATORS
# ============================================================


class UEFNBRIDGE_OT_connect(bpy.types.Operator):
    bl_idname = "uefn_bridge.connect"
    bl_label = "Connect to UEFN"
    bl_description = "Connect to UEFN bridge server"

    def execute(self, context):
        props = context.scene.uefn_bridge
        _st.port = props.port
        if do_connect():
            self.report({"INFO"}, f"Connected to UEFN ({_st.project_path})")
        else:
            self.report({"WARNING"}, _st.status)
        return {"FINISHED"}


class UEFNBRIDGE_OT_disconnect(bpy.types.Operator):
    bl_idname = "uefn_bridge.disconnect"
    bl_label = "Disconnect"
    bl_description = "Disconnect from UEFN"

    def execute(self, context):
        do_disconnect()
        return {"FINISHED"}


class UEFNBRIDGE_OT_send_scene(bpy.types.Operator):
    bl_idname = "uefn_bridge.send_scene"
    bl_label = "Send Full Scene"
    bl_description = "Export and send all mesh objects to UEFN"

    @classmethod
    def poll(cls, context):
        return _st.connected

    def execute(self, context):
        if do_send_scene():
            self.report({"INFO"}, _st.status)
        else:
            self.report({"WARNING"}, _st.status)
        return {"FINISHED"}


class UEFNBRIDGE_OT_send_changes(bpy.types.Operator):
    bl_idname = "uefn_bridge.send_changes"
    bl_label = "Send Changes"
    bl_description = "Send only if scene changed (geometry → full, transform → fast)"

    @classmethod
    def poll(cls, context):
        return _st.connected

    def execute(self, context):
        do_send_changes()
        self.report({"INFO"}, _st.status)
        return {"FINISHED"}


class UEFNBRIDGE_OT_send_selected(bpy.types.Operator):
    bl_idname = "uefn_bridge.send_selected"
    bl_label = "Send Selected"
    bl_description = "Export and send only selected mesh objects"

    @classmethod
    def poll(cls, context):
        return _st.connected and len(context.selected_objects) > 0

    def execute(self, context):
        if do_send_selected():
            self.report({"INFO"}, _st.status)
        else:
            self.report({"WARNING"}, _st.status)
        return {"FINISHED"}


class UEFNBRIDGE_OT_start_live(bpy.types.Operator):
    bl_idname = "uefn_bridge.start_live"
    bl_label = "Start Live Sync"
    bl_description = "Auto-sync changes to UEFN"

    @classmethod
    def poll(cls, context):
        return _st.connected and not _st.live_active

    def execute(self, context):
        do_start_live()
        return {"FINISHED"}


class UEFNBRIDGE_OT_stop_live(bpy.types.Operator):
    bl_idname = "uefn_bridge.stop_live"
    bl_label = "Stop Live Sync"

    def execute(self, context):
        do_stop_live()
        return {"FINISHED"}


class UEFNBRIDGE_OT_pull_transforms(bpy.types.Operator):
    bl_idname = "uefn_bridge.pull_transforms"
    bl_label = "Pull Transforms from UEFN"
    bl_description = "Request UEFN to push current actor transforms to Blender"

    @classmethod
    def poll(cls, context):
        return _st.connected

    def execute(self, context):
        try:
            _send("request_push_transforms", {
                "blender_port": _st.server_port,
            })
            self.report({"INFO"}, "Requested transforms from UEFN")
        except Exception as e:
            self.report({"WARNING"}, f"Pull failed: {e}")
        return {"FINISHED"}


class UEFNBridgeProperties(bpy.types.PropertyGroup):
    # No project_name field — the project is the .blend filename, fully automatic.
    port: bpy.props.IntProperty(
        name="Port",
        default=8790,
        min=1024,
        max=65535,
        description="UEFN bridge server port",
    )
    combine_meshes: bpy.props.BoolProperty(
        name="Combine Meshes",
        default=False,
        description="Merge the selection into ONE static mesh on import. Off (default) "
                    "keeps objects separate so each lands at its own Blender position.",
    )
# ============================================================
# N-PANEL (collapsible sub-panels)
# ============================================================


class UEFNBRIDGE_PT_main(bpy.types.Panel):
    bl_label = "UEFN Bridge"
    bl_idname = "UEFNBRIDGE_PT_main"
    bl_space_type = "VIEW_3D"
    bl_region_type = "UI"
    bl_category = "UEFN"

    def draw_header(self, context):
        self.layout.label(
            text="",
            icon="LINKED" if _st.connected else "UNLINKED")

    def draw(self, context):
        layout = self.layout
        props = context.scene.uefn_bridge

        box = layout.box()
        row = box.row()
        if _st.connected:
            if _st.live_active:
                row.label(text="Live Sync Active", icon="REC")
            else:
                row.label(text="Connected", icon="CHECKMARK")
        else:
            row.label(text="Disconnected", icon="X")

        box.label(text=_st.status, icon="INFO")

        if _st.last_error:
            ebox = box.box()
            ebox.alert = True
            ebox.label(text="Last error:", icon="ERROR")
            for line in _st.last_error.split("\n"):
                line = line.strip()
                if line:
                    ebox.label(text=line)

        if _st.project_name:
            box.label(text=f"Project: {_st.project_name}", icon="FILE_FOLDER")
        if _st.project_path:
            box.label(text=f"Path: {_st.project_path}", icon="FILEBROWSER")

        mesh_count = sum(1 for o in context.scene.objects if o.type == "MESH")
        box.label(text=f"Mesh objects: {mesh_count}", icon="MESH_DATA")

        if _st.send_count > 0:
            box.label(text=f"Total syncs: {_st.send_count}", icon="RECOVER_LAST")


class UEFNBRIDGE_PT_connection(bpy.types.Panel):
    bl_label = "Connection"
    bl_idname = "UEFNBRIDGE_PT_connection"
    bl_space_type = "VIEW_3D"
    bl_region_type = "UI"
    bl_category = "UEFN"
    bl_parent_id = "UEFNBRIDGE_PT_main"

    def draw(self, context):
        layout = self.layout
        props = context.scene.uefn_bridge

        if not _st.connected:
            auto = _blend_project_name()
            if auto:
                layout.label(text=f"Project: {auto}", icon="FILE_BLEND")
            else:
                layout.label(text="Save the .blend to name the project", icon="ERROR")
            layout.separator()
            layout.prop(props, "port")
            layout.operator("uefn_bridge.connect", icon="PLAY")
        else:
            layout.operator("uefn_bridge.disconnect", icon="CANCEL")


class UEFNBRIDGE_PT_export(bpy.types.Panel):
    bl_label = "Export & Send"
    bl_idname = "UEFNBRIDGE_PT_export"
    bl_space_type = "VIEW_3D"
    bl_region_type = "UI"
    bl_category = "UEFN"
    bl_parent_id = "UEFNBRIDGE_PT_main"

    @classmethod
    def poll(cls, context):
        return _st.connected

    def draw(self, context):
        layout = self.layout
        props = context.scene.uefn_bridge
        layout.prop(props, "combine_meshes")
        layout.separator()
        layout.operator("uefn_bridge.send_scene", icon="SCENE_DATA")
        layout.operator("uefn_bridge.send_changes", icon="FILE_REFRESH")
        layout.operator("uefn_bridge.send_selected", icon="RESTRICT_SELECT_OFF")


class UEFNBRIDGE_PT_live(bpy.types.Panel):
    bl_label = "Live Sync"
    bl_idname = "UEFNBRIDGE_PT_live"
    bl_space_type = "VIEW_3D"
    bl_region_type = "UI"
    bl_category = "UEFN"
    bl_parent_id = "UEFNBRIDGE_PT_main"

    @classmethod
    def poll(cls, context):
        return _st.connected

    def draw(self, context):
        layout = self.layout

        if _st.live_active:
            layout.operator("uefn_bridge.stop_live", icon="PAUSE")
            layout.label(text="Every Ctrl+S syncs to UEFN", icon="REC")
        else:
            layout.operator("uefn_bridge.start_live", icon="PLAY")

        layout.separator()
        layout.operator("uefn_bridge.pull_transforms", icon="IMPORT")

        box = layout.box()
        box.label(text="Save (Ctrl+S) triggers sync", icon="FILE_TICK")
        box.label(text="Geometry change: full re-export", icon="MESH_DATA")
        box.label(text="Transform only: fast update", icon="ORIENTATION_LOCAL")
        box.label(text="Pull: get positions from UEFN", icon="IMPORT")


class UEFNBRIDGE_PT_info(bpy.types.Panel):
    bl_label = "Info"
    bl_idname = "UEFNBRIDGE_PT_info"
    bl_space_type = "VIEW_3D"
    bl_region_type = "UI"
    bl_category = "UEFN"
    bl_parent_id = "UEFNBRIDGE_PT_main"
    bl_options = {"DEFAULT_CLOSED"}

    def draw(self, context):
        layout = self.layout
        layout.label(text=f"UEFN Blender Bridge v{ADDON_VERSION}")
        layout.label(text="by KiKoZl - Surprise Co.")


# ============================================================
# REGISTER
# ============================================================

_classes = (
    UEFNBridgeProperties,
    UEFNBRIDGE_OT_connect,
    UEFNBRIDGE_OT_disconnect,
    UEFNBRIDGE_OT_send_scene,
    UEFNBRIDGE_OT_send_changes,
    UEFNBRIDGE_OT_send_selected,
    UEFNBRIDGE_OT_start_live,
    UEFNBRIDGE_OT_stop_live,
    UEFNBRIDGE_OT_pull_transforms,
    UEFNBRIDGE_PT_main,
    UEFNBRIDGE_PT_connection,
    UEFNBRIDGE_PT_export,
    UEFNBRIDGE_PT_live,
    UEFNBRIDGE_PT_info,
)


def register():
    for cls in _classes:
        bpy.utils.register_class(cls)
    bpy.types.Scene.uefn_bridge = bpy.props.PointerProperty(
        type=UEFNBridgeProperties)
    if _on_load not in bpy.app.handlers.load_pre:
        bpy.app.handlers.load_pre.append(_on_load)
    _log("UEFN Bridge addon registered")


def unregister():
    if _on_load in bpy.app.handlers.load_pre:
        bpy.app.handlers.load_pre.remove(_on_load)
    do_disconnect()
    for cls in reversed(_classes):
        bpy.utils.unregister_class(cls)
    if hasattr(bpy.types.Scene, "uefn_bridge"):
        del bpy.types.Scene.uefn_bridge
