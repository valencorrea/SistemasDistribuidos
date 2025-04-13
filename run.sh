#!/bin/bash

# Crear directorios necesarios
mkdir -p data results

# Verificar si los archivos CSV existen
if [ ! -f "data/movies_metadata.csv" ]; then
    echo "Error: No se encuentra data/movies_metadata.csv"
    echo "Por favor, descarga el dataset de Kaggle y coloca los archivos CSV en el directorio 'data'"
    exit 1
fi

# Construir y levantar los contenedores
docker-compose up --build -d

echo "Sistema iniciado. Para enviar archivos, ejecuta:"
echo "python -m client.file_sender"

# Mostrar logs
docker-compose logs -f 