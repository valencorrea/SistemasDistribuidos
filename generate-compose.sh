#!/bin/bash

if [ "$#" -lt 2 ]; then
    echo "Uso: $0 <archivo_salida> <workers_twentieth_century> <workers_main_movie> <workers_esp_production> <workers_no_colab_production> <workers_sentiment> <workers_arg_production> <workers_credits> <workers_credits>"
    exit 1
fi

output_file=$1
file=$2
workers_twentieth_century=$3
workers_main_movie=$4
workers_esp_production=$5
workers_no_colab_production=$6
workers_sentiment=$7
workers_arg_production=$8
workers_credits=$9
workers_ratings=$10

# Si sólo se pasó uno
if [ -z "$workers_main_movie" ]; then
    workers_main_movie=$workers_twentieth_century
    workers_esp_production=$workers_twentieth_century
    workers_no_colab_production=$workers_twentieth_century
    workers_sentiment=$workers_twentieth_century
    workers_arg_production=$workers_twentieth_century
    workers_credits=$workers_twentieth_century
    workers_ratings=$workers_twentieth_century
fi

echo "Nombre del archivo de salida: $output_file"
echo "Tipo de prueba: $file"
echo "Cantidad de workers de twentieth_century_filter: $workers_twentieth_century"
echo "Cantidad de workers de arg_production_filter: $workers_arg_production"
echo "Cantidad de workers de main_movie_filter: $workers_main_movie"
echo "Cantidad de workers de esp_production_filter: $workers_esp_production"
echo "Cantidad de workers de no_colab_production_filter: $workers_no_colab_production"
echo "Cantidad de workers de sentiment_filter: $workers_sentiment"
echo "Cantidad de workers de workers_credits: $workers_credits"
echo "Cantidad de workers de workers_ratings: $workers_ratings"

python3 docker-compose-generator.py "$output_file" "$file" "$workers_twentieth_century" "$workers_main_movie" "$workers_esp_production" "$workers_no_colab_production" "$workers_sentiment" "$workers_arg_production" "$workers_credits" "$workers_ratings"
