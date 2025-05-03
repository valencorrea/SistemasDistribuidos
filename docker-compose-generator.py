import sys
import yaml

def generate_docker_yaml(workers_twentieth_century, workers_main_movie, workers_esp_production, workers_no_colab_productions,
                         workers_sentiment, workers_arg_production, workers_credits, test_mode):
    ratings_source_file = "ratings_small.csv" if test_mode else "ratings.csv"

    common_volumes = [
        "./files/movies_metadata.csv:/root/files/movies_metadata.csv",
        "./files/credits.csv:/root/files/credits.csv",
        "./middleware:/app/middleware",
        f"./files/{ratings_source_file}:/root/files/ratings.csv"
    ]
    print("ratings: ", ratings_source_file)

    template = {
        "services": {
            "rabbitmq": {
                "build": {
                    "context": "./rabbitmq",
                    "dockerfile": "rabbitmq.dockerfile",
                },
                "ports": ["15672:15672"],
                "healthcheck": {
                    "test": "CMD curl -f http://localhost:15672",
                    "interval": "10s",
                    "timeout": "5s",
                    "retries": 10
                },
                "volumes": ["./rabbitmq/config.ini:/config.ini"]
            },
            "client_decodifier": {
                "build": {
                    "context": ".",
                    "dockerfile": "client_decodifier/client_decodifier.dockerfile",
                },
                "depends_on": ["rabbitmq"],
                "ports": ["50000:50000"],
                "volumes": ["./middleware:/app/middleware"]
            },
            "client": {
                "container_name": "client",
                "build": {
                    "context": ".",
                    "dockerfile": "client/client.dockerfile",
                },
                "environment": ["PYTHONUNBUFFERED=1", "DECODIFIER_HOST=client_decodifier", "DECODIFIER_PORT=50000"],
                "depends_on": ["client_decodifier"],
                "volumes": common_volumes
            },
            "twentieth_century_arg_esp_aggregator": {
                "build": {
                    "context": ".",
                    "dockerfile": "aggregator/twentieth_century_arg_esp_aggregator/aggregator.dockerfile",
                },
                "depends_on": ["rabbitmq"],
                "links": ["rabbitmq"],
                "environment": ["PYTHONUNBUFFERED=1"]
            },
            "twentieth_century_arg_aggregator": {
                "build": {
                    "context": ".",
                    "dockerfile": "aggregator/twentieth_century_arg_aggregator/aggregator.dockerfile",
                },
                "depends_on": ["rabbitmq"],
                "links": ["rabbitmq"],
                "environment": ["PYTHONUNBUFFERED=1"]
            },
            "top_5_countries_aggregator": {
                "build": {
                    "context": ".",
                    "dockerfile": "aggregator/top_aggregator/aggregator.dockerfile",
                },
                "depends_on": ["rabbitmq"],
                "links": ["rabbitmq"],
                "environment": ["PYTHONUNBUFFERED=1"]
            },
            "top_10_credits_aggregator": {
                "build": {
                    "context": ".",
                    "dockerfile": "aggregator/top_10_credits_aggregator/aggregator.dockerfile",
                },
                "depends_on": ["rabbitmq"],
                "links": ["rabbitmq"],
                "environment": ["PYTHONUNBUFFERED=1"]
            },
            "credits_joiner": {
                "build": {
                    "context": ".",
                    "dockerfile": "joiner/credits/credits_joiner.dockerfile",
                },
                "depends_on": ["rabbitmq"],
                "links": ["rabbitmq"],
                "environment": ["PYTHONUNBUFFERED=1"]
            },
            "ratings_joiner": {
                "build": {
                    "context": ".",
                    "dockerfile": "joiner/ratings/rating_joiner.dockerfile",
                },
                "depends_on": ["rabbitmq"],
                "links": ["rabbitmq"],
                "environment": ["PYTHONUNBUFFERED=1"]
            },
            "sentiment_aggregator": {
                "build": {
                    "context": ".",
                    "dockerfile": "aggregator/sentiment_aggregator/aggregator.dockerfile",
                },
                "depends_on": ["rabbitmq"],
                "links": ["rabbitmq"],
                "environment": ["PYTHONUNBUFFERED=1"]
            },
            "test": {
                "build": {
                    "context": ".",
                    "dockerfile": "test/integration.dockerfile",
                },
            }
        }
    }

    worker_definitions = {
        "twentieth_century_filter": ("filters/twentieth_century/twentieth_century_filter.dockerfile", workers_twentieth_century),
        "arg_production_filter": ("filters/arg_production/arg_production_filter.dockerfile", workers_arg_production),
        "workers_credits": ("joiner/credits/credits_joiner.dockerfile", workers_credits),
        "main_movie_filter": ("filters/main_movie_filter/main_movie_filter.dockerfile", workers_main_movie),
        "esp_production_filter": ("filters/esp_production/esp_production_filter.dockerfile", workers_esp_production),
        "no_colab_productions_filter": ("filters/no_colab_productions/no_colab_productions_filter.dockerfile", workers_no_colab_productions),
        "sentiment_filter": ("filters/sentiment_analizer/sentiment_analizer.dockerfile", workers_sentiment)
    }

    for service_name, (dockerfile_path, worker_count) in worker_definitions.items():
        # Servicio principal
        template["services"][service_name] = {
            "build": {
                "context": ".",
                "dockerfile": dockerfile_path,
            },
            "image": f"{service_name}:latest" if service_name == "sentiment_filter" else None,
            "depends_on": ["rabbitmq"],
            "links": ["rabbitmq"],
            "environment": ["PYTHONUNBUFFERED=1"]
        }
        if template["services"][service_name]["image"] is None:
            del template["services"][service_name]["image"]

        # Workers adicionales
        for i in range(1, worker_count):
            worker_name = f"{service_name}_{i}"
            template["services"][worker_name] = {
                "image": f"{service_name}:latest",
                "depends_on": ["rabbitmq"],
                "links": ["rabbitmq"],
                "environment": ["PYTHONUNBUFFERED=1"]
            }

    return template


def dump_yaml_to_file(template, filename):
    with open(filename, "w") as file:
        yaml.dump(template, file, default_flow_style=False)


if __name__ == "__main__":
    print("Se inició el generador de docker-compose")
    if len(sys.argv) != 10:
        print("Uso: python3 docker-compose-generator.py <output_file> <short:long> <workers_twentieth_century> <workers_main_movie> <workers_esp_production> <workers_no_colab_productions> <workers_sentiment> <workers_arg_production> <workers_credits>")
        sys.exit(1)

    compose_filename = sys.argv[1]
    _file = sys.argv[2]
    _workers_twentieth_century = int(sys.argv[3])
    _workers_main_movie = int(sys.argv[4])
    _workers_esp_production = int(sys.argv[5])
    _workers_no_colab_productions = int(sys.argv[6])
    _workers_sentiment = int(sys.argv[7])
    _workers_arg_production = int(sys.argv[8])
    _workers_credits = int(sys.argv[9])

    if (_workers_twentieth_century < 1 or _workers_main_movie < 1 or _workers_esp_production < 1
            or _workers_no_colab_productions < 1 or _workers_sentiment < 1 or _workers_arg_production < 1 or _workers_credits < 1):
        print("Debe haber al menos 1 worker por servicio.")
        sys.exit(1)

    docker_compose_template = generate_docker_yaml(
        _workers_twentieth_century, _workers_main_movie, _workers_esp_production, _workers_no_colab_productions, _workers_sentiment, _workers_arg_production, _workers_credits, test_mode=True if _file == "short" else False
    )
    dump_yaml_to_file(docker_compose_template, compose_filename)
