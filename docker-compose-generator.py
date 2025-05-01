import sys
import yaml

def generate_docker_yaml(workers_twentieth_century, workers_main_movie, workers_esp_production, workers_top5,
                         workers_sentiment, files):
    template = {
        "version": "3",
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
            "client": {
                "build": {
                    "context": ".",
                    "dockerfile": "client/client.dockerfile",
                },
                "depends_on": ["rabbitmq"],
                "links": ["rabbitmq"],
                "environment": [
                    "PYTHONUNBUFFERED=1"
                    "files=" + files
                ]
            },
            "twentieth_century_arg_production_filter": {
                "build": {
                    "context": ".",
                    "dockerfile": "aggregator/twentieth_century_arg_production_filter/aggregator.dockerfile",
                },
                "depends_on": ["rabbitmq"],
                "links": ["rabbitmq"],
                "environment": ["PYTHONUNBUFFERED=1"]
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
            "top_5_countries_aggregator": {
                "build": {
                    "context": ".",
                    "dockerfile": "aggregator/top_aggregator/aggregator.dockerfile",
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
        "twentieth_century_arg_production_filter": ("filters/twentieth_century_arg_production/twentieth_century_arg_production_filter.dockerfile", workers_twentieth_century),
        "main_movie_filter": ("filters/main_movie_filter/main_movie_filter.dockerfile", workers_main_movie),
        "esp_production_filter": ("filters/esp_production/esp_production_filter.dockerfile", workers_esp_production),
        "top_5_countries_filter": ("filters/top_5_local/top_5_local.dockerfile", workers_top5),
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
    if len(sys.argv) != 8:
        print("Uso: python3 docker-compose-generator.py <output_file> <short:long> <workers_twentieth_century> <workers_main_movie> <workers_esp_production> <workers_top5> <workers_sentiment>")
        sys.exit(1)

    compose_filename = sys.argv[1]
    _file = sys.argv[2]
    _workers_twentieth_century = int(sys.argv[3])
    _workers_main_movie = int(sys.argv[4])
    _workers_esp_production = int(sys.argv[5])
    _workers_top5 = int(sys.argv[6])
    _workers_sentiment = int(sys.argv[7])

    if (_workers_twentieth_century < 1 or _workers_main_movie < 1 or _workers_esp_production < 1
            or _workers_top5 < 1 or _workers_sentiment < 1):
        print("Debe haber al menos 1 worker por servicio.")
        sys.exit(1)

    docker_compose_template = generate_docker_yaml(
        _workers_twentieth_century, _workers_main_movie, _workers_esp_production, _workers_top5, _workers_sentiment, _file
    )
    dump_yaml_to_file(docker_compose_template, compose_filename)
