Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
scriptPath = fso.GetParentFolderName(WScript.ScriptFullName) & "\start-aqua-msk.ps1"
shell.Run "powershell -NoProfile -ExecutionPolicy Bypass -File """ & scriptPath & """", 0, False
