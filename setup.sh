#!/usr/bin/env bash

command -v virtualenv >/dev/null 2>&1 || { echo >&2 "I require virtualenv but it's not installed.  Aborting."; exit 1; }

root_dir=$(cd -P -- "$(dirname -- "$0")" && pwd -P)


os=$(uname -s)
os_hardware=$(uname -m)
venv_dir="$root_dir/.venv-$os-$os_hardware"


if [ ! -f "$venv_dir/bin/python" ]
then
    echo "configuring virtualenv $venv_dir ..."
    virtualenv -q "$venv_dir" || { echo >&2 "unable to configure the virtualenv for the project in $venv_dir"; exit 1; }
fi


"$venv_dir/bin/pip" install -q -r "$root_dir/requirements.txt"

if [ $? -ne 0 ]
then
    >&2 echo "failed to install pip packages"
    exit 1
fi


mp_config_file="$root_dir/config.yaml"
if [ ! -f "$mp_config_file" ]
then
echo "setting up admin password"
cat >    "$root_dir/config.yaml" << EOF
ADMIN_AUTH: '1a1dc91c907325c69271ddf0c944bc72'

EOF
fi