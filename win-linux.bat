@echo off
set ROOT_DIR=%~dp0
echo starting Windows subsystem for Linux in %ROOT_DIR%
REM wsl -e /bin/bash -c "cd $(wslpath '%ROOT_DIR%python');/bin/bash"
wsl -e /bin/bash -l -c "cd $(wslpath '%ROOT_DIR%');/bin/bash"
