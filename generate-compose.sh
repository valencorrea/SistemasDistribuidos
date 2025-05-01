#!/bin/bash

if [ "$#" -lt 2 ]; then
    echo "Uso: $0 <archivo_salida> <workers_twentieth_century> [<workers_main_movie> <workers_esp_production> <workers_top5> <workers_sentiment>]"
    exit 1
fi

output_file=$1
file=$2
workers_twentieth_century=$3
workers_main_movie=$4
workers_esp_production=$5
workers_top5=$6
workers_sentiment=$7
workers_credits=$7

# Si sólo se pasó uno
if [ -z "$workers_main_movie" ]; then
    workers_main_movie=$workers_twentieth_century
    workers_esp_production=$workers_twentieth_century
    workers_top5=$workers_twentieth_century
    workers_sentiment=$workers_twentieth_century
    workers_credits=$workers_twentieth_century
fi

echo "Nombre del archivo de salida: $output_file"
echo "Tipo de prueba: $file"
echo "Cantidad de workers de twentieth_century_arg_production_filter: $workers_twentieth_century"
echo "Cantidad de workers de main_movie_filter: $workers_main_movie"
echo "Cantidad de workers de esp_production_filter: $workers_esp_production"
echo "Cantidad de workers de top_5_countries_filter: $workers_top5"
echo "Cantidad de workers de sentiment_filter: $workers_sentiment"
echo "Cantidad de workers de workers_credits: workers_credits"

python3 docker-compose-generator.py "$output_file" "$file" "$workers_twentieth_century" "$workers_main_movie" "$workers_esp_production" "$workers_top5" "$workers_sentiment" "$workers_credits"
