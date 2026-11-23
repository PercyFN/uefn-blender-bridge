# Local session setup

This fork pairs the Blender add-on and the UEFN editor script over authenticated loopback HTTP. Keep the session directory and token in your local user profile, outside project content and version control.

1. Install `BlenderAddon/uefn_bridge` as a Blender add-on. Use `UEFNPlugin/uefn_blender_bridge.py` from the same checkout.
2. Create a per-project directory with an `exchange` subdirectory and `session_token` file. The token file contains 64 random hexadecimal characters. For example, in PowerShell on Windows:

   ```powershell
   $BridgeSession = Join-Path $env:LOCALAPPDATA 'UEFNBlenderBridge\MyProject'
   New-Item -ItemType Directory -Path (Join-Path $BridgeSession 'exchange') -Force | Out-Null
   $BridgeTokenFile = Join-Path $BridgeSession 'session_token'
   if (!(Test-Path -LiteralPath $BridgeTokenFile)) {
       $BridgeToken = [byte[]]::new(32)
       $BridgeRandom = [System.Security.Cryptography.RandomNumberGenerator]::Create()
       try { $BridgeRandom.GetBytes($BridgeToken) } finally { $BridgeRandom.Dispose() }
       $BridgeHex = [System.BitConverter]::ToString($BridgeToken).Replace('-', '').ToLowerInvariant()
       [System.IO.File]::WriteAllText($BridgeTokenFile, $BridgeHex)
   }
   $env:UEFN_BLENDER_BRIDGE_SESSION_DIR = $BridgeSession
   ```

3. Set `UEFN_BLENDER_BRIDGE_SESSION_DIR` in both processes to that directory. Launch Blender from the shell where you set it. In an already-open UEFN editor you can set `os.environ['UEFN_BLENDER_BRIDGE_SESSION_DIR']` in the editor's Python session.
4. Open the intended UEFN level and run `uefn_blender_bridge.py` once. In the saved Blender project, open the **UEFN** sidebar, click **Connect**, and use **Send Selected** for the initial transfer. Use **Start Live Sync** to enable save-driven updates.

The first transfer creates bridge-owned static mesh assets and actors. Later mesh edits reimport into the existing asset. UEFN sends linked actor transforms back to Blender. Reconnect after switching files or levels. Closing the UEFN dashboard stops the listener.

The fork removes the remote Python execution hook, automated deletion, and the experimental Bake & Send feature. Source FBX files remain in the exchange directory for reimport. Keep the exchange directory for assets you still edit.
