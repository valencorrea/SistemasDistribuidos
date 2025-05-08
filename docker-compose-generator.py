import sys

import yaml


def generate_docker_yaml(config):
    workers = config["workers"]
    clients = config["clients"]
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
        }
    }
    test_set = False
    # Add multiple clients
    for client in clients:
        cid = client["id"]
        path = client["files_path"]
        name = f"client_{cid}"
        template["services"][name] = {
            "container_name": name,
            "build": {
                "context": ".",
                "dockerfile": "client/client.dockerfile",
            },
            "environment": [
                "PYTHONUNBUFFERED=1",
                "DECODIFIER_HOST=client_decodifier",
                "DECODIFIER_PORT=50000",
                f"CLIENT_ID={cid}"
            ],
            "depends_on": ["client_decodifier"],
            "volumes": [
                f"{path}/movies_metadata.csv:/root/files/movies_metadata.csv",
                f"{path}/credits.csv:/root/files/credits.csv",
                f"{path}/ratings.csv:/root/files/ratings.csv",
                "./middleware:/app/middleware"
            ]
        }
        if not test_set and config["test"] != False:
            template["services"]["test"] = {
                "build": {
                    "context": ".",
                    "dockerfile": "test/integration.dockerfile"
                },
                "volumes": [
                      f"{path}/movies_metadata.csv:/root/files/movies_metadata.csv",
                     f"{path}/credits.csv:/root/files/credits.csv",
                    f"{path}/ratings.csv:/root/files/ratings.csv"
                ]
            }
            test_set = True

    # Worker definitions
    worker_definitions = {
        "twentieth_century_filter": "filters/twentieth_century/twentieth_century_filter.dockerfile",
        "arg_production_filter": "filters/arg_production/arg_production_filter.dockerfile",
        "credits_joiner": "joiner/credits/credits_joiner.dockerfile",
        "ratings_joiner": "joiner/ratings/ratings_joiner.dockerfile",
        "main_movie_filter": "filters/main_movie_filter/main_movie_filter.dockerfile",
        "esp_production_filter": "filters/esp_production/esp_production_filter.dockerfile",
        "no_colab_productions_filter": "filters/no_colab_productions/no_colab_productions_filter.dockerfile",
        "sentiment_filter": "filters/sentiment_analizer/sentiment_analizer.dockerfile"
    }

    for name, dockerfile in worker_definitions.items():
        count = workers.get(name.split('_filter')[0] if '_filter' in name else name.split('_joiner')[0], 1)

        if count == 0:
            continue

        template["services"][name] = {
            "build": {
                "context": ".",
                "dockerfile": dockerfile
            },
            "depends_on": ["rabbitmq"],
            "links": ["rabbitmq"],
            "environment": ["PYTHONUNBUFFERED=1"],
            "image": f"{name}:latest"
        }

        if name == "credits_joiner":
            template["services"][name]["volumes"] = ["./files:/root/files"]

        for i in range(1, count):
            template["services"][f"{name}_{i}"] = {
                "image": f"{name}:latest",
                "depends_on": ["rabbitmq"],
                "links": ["rabbitmq"],
                "environment": ["PYTHONUNBUFFERED=1"]
            }

    aggregator_services = {
        "twentieth_century_arg_esp_aggregator": "aggregator/twentieth_century_arg_esp_aggregator/aggregator.dockerfile",
        "twentieth_century_arg_aggregator": "aggregator/twentieth_century_arg_aggregator/aggregator.dockerfile",
        "top_5_countries_aggregator": "aggregator/top_aggregator/aggregator.dockerfile",
        "top_10_credits_aggregator": "aggregator/top_10_credits_aggregator/aggregator.dockerfile",
        "best_and_worst_ratings_aggregator": "aggregator/best_and_worst_ratings_aggregator/aggregator.dockerfile",
        "sentiment_aggregator": "aggregator/sentiment_aggregator/aggregator.dockerfile"
    }

    for name, dockerfile in aggregator_services.items():
        template["services"][name] = {
            "build": {
                "context": ".",
                "dockerfile": dockerfile
            },
            "depends_on": ["rabbitmq"],
            "links": ["rabbitmq"],
            "environment": ["PYTHONUNBUFFERED=1"]
        }

    return template


def dump_yaml_to_file(template, filename):
    with open(filename, "w") as file:
        yaml.dump(template, file, default_flow_style=False)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 docker-compose-generator.py <archivo_configuracion>")
        sys.exit(1)

    with open(sys.argv[1], "r") as f:
        config = yaml.safe_load(f)

    for key in ["output_file", "clients", "workers"]:
        if key not in config:
            print(f"Missing key '{key}' in config file.")
            sys.exit(1)

    docker_compose = generate_docker_yaml(config)
    dump_yaml_to_file(docker_compose, config["output_file"])
    print(f"Docker Compose generated at {config['output_file']}")
