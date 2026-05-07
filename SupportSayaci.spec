# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path

project_root = Path.cwd()


a = Analysis(
    ['src/app_tk.py'],
    pathex=[str(project_root)],
    binaries=[],
    datas=[],
    hiddenimports=['customtkinter', 'tkcalendar', 'matplotlib.backends.backend_tkagg'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='SupportSayaci',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
