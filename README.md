# Sistema Distribuido de Análisis de Películas

Sistema distribuido para el análisis de datos de películas utilizando comunicación TCP y procesamiento paralelo.

## Arquitectura

El sistema consta de los siguientes componentes:

### 1. Middleware (TCP)
- Actúa como punto central de comunicación
- Distribuye chunks de datos entre workers disponibles
- Mantiene registro del progreso de cada worker
- Asegura distribución equitativa de la carga de trabajo

### 2. Workers de Filtrado
- 3 instancias para filtrar películas post-2000
- 3 instancias para filtrar películas argentinas/españolas
- Procesan chunks de datos en paralelo
- Mantienen conteo de líneas procesadas

### 3. Agregador
- Combina resultados de todos los workers
- Evita duplicados en los resultados
- Guarda resultados cuando se completa el procesamiento
- Mantiene estado en memoria hasta completar todo el archivo

### 4. Cliente
- Lee y envía archivos en chunks
- Proporciona información sobre el total de líneas
- Envía archivos en orden: movies → credits → ratings

## Diagrama de Comunicación

```
Cliente ---> Middleware ---> Workers (x6) ---> Agregador
                ^              |
                |              |
                +--------------+
```

## Requisitos

- Docker y Docker Compose
- Dataset de películas de Kaggle (archivos CSV)
- Python 3.9 o superior (para el cliente)

## Estructura del Proyecto

```
.
├── aggregator/              # Código del agregador
├── client/                 # Cliente para envío de archivos
├── data/                   # Directorio para archivos CSV
├── docker/                 # Dockerfiles
│   ├── aggregator.Dockerfile
│   ├── middleware.Dockerfile
│   └── worker.Dockerfile
├── middleware/             # Código del middleware TCP
├── results/               # Directorio para resultados
├── worker/                # Código de los workers
├── docker-compose.yml     # Configuración de servicios
├── requirements.txt       # Dependencias Python
├── run.sh                # Script de inicio
└── stop.sh               # Script de detención

```

## Instalación y Uso

1. Clonar el repositorio:
```bash
git clone <repositorio>
cd distribuidos
```

2. Preparar los datos:
```bash
mkdir -p data
# Colocar los siguientes archivos en el directorio data/:
# - movies_metadata.csv
# - ratings.csv
# - credits.csv
```

3. Dar permisos de ejecución a los scripts:
```bash
chmod +x run.sh stop.sh
```

4. Iniciar el sistema:
```bash
./run.sh
```

5. En otra terminal, enviar los archivos:
```bash
python -m client.file_sender
```

6. Para detener el sistema:
```bash
./stop.sh
```

## Configuración Docker

El sistema utiliza Docker Compose para orquestar los siguientes servicios:

### Middleware
```yaml
middleware:
  build:
    context: .
    dockerfile: docker/middleware.Dockerfile
  ports:
    - "5000:5000"
```
- Puerto 5000 expuesto para conexiones TCP
- Volumen montado para acceso a datos

### Workers
```yaml
movie_filter_post2000:
  environment:
    - FILTER_TYPE=post_2000
  deploy:
    replicas: 3
```
- 3 réplicas por tipo de filtro
- Configuración vía variables de entorno

### Agregador
```yaml
aggregator:
  volumes:
    - ./results:/app/results
```
- Volumen para guardar resultados
- Depende del middleware

## Resultados

Los resultados se guardan en el directorio `results/` en dos archivos:

- `post_2000_results.json`: Películas posteriores al año 2000
- `argentina_spain_results.json`: Películas argentinas y españolas

## Características Técnicas

- Comunicación TCP pura sin frameworks adicionales
- Procesamiento paralelo con múltiples workers
- Control de progreso por líneas procesadas
- Manejo de estado distribuido
- Tolerancia a fallos básica
- Distribución equitativa de carga

## Limitaciones

- No implementa recuperación automática de workers caídos
- El agregador mantiene todos los resultados en memoria
- No soporta redistribución dinámica de trabajo
- Requiere que los archivos se envíen en orden específico

## Contribuir

1. Fork del repositorio
2. Crear rama para feature (`git checkout -b feature/nombre`)
3. Commit cambios (`git commit -am 'Agregar feature'`)
4. Push a la rama (`git push origin feature/nombre`)
5. Crear Pull Request
