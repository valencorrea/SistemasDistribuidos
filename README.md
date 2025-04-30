## Sistemas Distribuidos I: Movies Analysis

| Nombre alumno            | Padron | Mail                      | Github                                     |              
|--------------------------|--------|---------------------------|--------------------------------------------|
| Agustin Ariel Andrade    | 104046 | aandrade@fi.uba.ar        | [GitHub](https://github.com/AgussAndrade)  |
| Pablo Salvador Dimartino | 103963 | pdimartino@fi.uba.ar      | [GitHub](https://github.com/psdimartino)   |
| Valentina Laura Correa   | 104415 | vcorrea@fi.uba.ar         | [GitHub](https://github.com/valencorrea)   |

### Requerimientos funcionales
- Se solicita un sistema distribuido que analice la información de películas y los ratings de sus
espectadores en plataformas como iMDb.
- Los ratings son un valor numérico de 1 al 5. Las películas tienen información como género,
fecha de estreno, países involucrados en la producción, idioma, presupuesto e ingreso.
- Se debe obtener:
  1. Películas y sus géneros de los años 2000 con producción Argentina y Española.
  2. Top 5 de países que más dinero han invertido en producciones sin colaborar con otros
  países.
  3. Película de producción Argentina estrenada a partir del 2000, con mayor y con menor
  promedio de rating.
  4. Top 10 de actores con mayor participación en películas de producción Argentina con
  fecha de estreno posterior al 2000
  5. Average de la tasa ingreso/presupuesto de peliculas con overview de sentimiento positivo
  vs. sentimiento negativo

### Requerimientos No Funcionales
- El sistema debe estar optimizado para entornos multicomputadoras
- Se debe soportar el incremento de los elementos de cómputo para
escalar los volúmenes de información a procesar
- Se requiere del desarrollo de un Middleware para abstraer la
comunicación basada en grupos.
- Se debe soportar una única ejecución del procesamiento y proveer
graceful quit frente a señales SIGTERM.

### Datasets, notebook patrón y librerías
- [Dataset](https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset)
- [Notebook de resultados](https://www.kaggle.com/code/gabrielrobles/fiuba-distribuidos-1-the-movies)
- [Herramientas de procesamiento de lenguaje natural](https://huggingface.co/docs/transformers/en/main_classes/pipelines)


### RabbitMQ
Url de conexion: http://localhost:15672 
User: fiuba
Password: fiuba

### Comandos utiles

Limpieza de cache:

docker builder prune --force

docker buildx prune --force

docker system prune --all --force --volumes


