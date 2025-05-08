#!/bin/bash

if [ "$#" -lt 1 ]; then
    echo "Uso: $0 <archivo_configuracion>"
    exit 1
fi

cfg_file=$1

echo "Tipo de prueba: $cfg_file"

python3 docker-compose-generator.py "$cfg_file"