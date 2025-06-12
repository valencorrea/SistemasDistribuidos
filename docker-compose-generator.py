import sys

import yaml


def generate_docker_yaml(config):
    workers = config["workers"]
    clients = config["clients"]
    aggregators = config["aggregators"]

    template = {
        "services": {
            "worker": {
                "build": {
                    "context": ".",
                    "dockerfile": "worker/worker.dockerfile",
                },
                "image": "worker:latest"
            },
            "rabbitmq": {
                "build": {
                    "context": "./rabbitmq",
                    "dockerfile": "rabbitmq.dockerfile",
                },
                "ports": ["15672:15672"],
                "healthcheck": {
                    "test": "rabbitmq-diagnostics check_port_connectivity",
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
                "depends_on": {
                    "rabbitmq": {
                        "condition": "service_healthy"
                    },
                    "worker": {
                        "condition": "service_started"
                    }
                },
                "ports": ["50000:50000"],
                "healthcheck": {
                    "test": 'netstat -ltn | grep -c 5000',
                    "interval": "5s",
                    "timeout": "2s",
                    "retries": 10
                }
            },
        }
    }

    for client in clients:
        cid = client["id"]
        path = client["files_path"]
        results_path = client["files_path"].replace("files", "results")
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
            "depends_on": {
                    "client_decodifier": {
                        "condition": "service_healthy"
                    }
                },
            "volumes": [
                f"{path}/movies_metadata.csv:/root/files/movies_metadata.csv",
                f"{path}/credits.csv:/root/files/credits.csv",
                f"{path}/ratings.csv:/root/files/ratings.csv",
                f"{results_path}/results.json:/root/results/results.json"
            ]
        }

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

        worker_conf = workers.get(name.split('_filter')[0] if '_filter' in name else name.split('_joiner')[0], 1)
        if isinstance(worker_conf, dict):
            count = worker_conf.get("count", 1)
            log_level = worker_conf.get("log_level", "INFO")
        else:
            count = worker_conf
            log_level = "INFO"

        if count == 0:
            continue

        template["services"][name] = {
            "build": {
                "context": ".",
                "dockerfile": dockerfile
            },
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                },
                "worker": {
                    "condition": "service_started"
                }
            },
            "links": ["rabbitmq"],
            "environment": ["PYTHONUNBUFFERED=1", f"LOG_LEVEL={log_level}"],
            "image": f"{name}:latest"
        }

        for i in range(1, count):
            template["services"][f"{name}_{i}"] = {
                "image": f"{name}:latest",
                "depends_on": {
                    "rabbitmq": {
                        "condition": "service_healthy"
                    },
                    "worker": {
                        "condition": "service_started"
                    }
                },
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
        config_key = name.replace("_aggregator", "")
        agg_conf = aggregators.get(config_key, {})
        generate = agg_conf.get("generate", False)
        if not generate:
            continue
        log_level = agg_conf.get("log_level", "INFO")
        template["services"][name] = {
            "build": {
                "context": ".",
                "dockerfile": dockerfile
            },
            "depends_on": {
                "rabbitmq": {
                    "condition": "service_healthy"
                },
                "worker": {
                    "condition": "service_started"
                }
            },
            "links": ["rabbitmq"],
            "environment": [
                "PYTHONUNBUFFERED=1",
                f"LOG_LEVEL={log_level}"
            ]
        }

    template["services"]["monitor"] = {
        "build": {
            "context": ".",
            "dockerfile": "monitor/monitor.dockerfile"
        },
        "ports": ["50001:50001"],
        "environment": [
            "PYTHONUNBUFFERED=1",
            "MONITOR_PORT=50001",
            "HEARTBEAT_INTERVAL=5000",
            "HEARTBEAT_TIMEOUT=15000"
        ],
        "volumes": ["/var/run/docker.sock:/var/run/docker.sock"]
    }
    
    for service_name in template["services"]:
        if service_name != "monitor" and service_name != "rabbitmq":
            if "environment" not in template["services"][service_name]:
                template["services"][service_name]["environment"] = []
            
            template["services"][service_name]["environment"].extend([
                "MONITOR_HOST=monitor",
                "MONITOR_PORT=50001",
                "HEARTBEAT_INTERVAL=5000",
                f"SERVICE_NAME={service_name}"
            ])
            
            if "depends_on" not in template["services"][service_name]:
                template["services"][service_name]["depends_on"] = {}

            if isinstance(template["services"][service_name]["depends_on"], list):
                template["services"][service_name]["depends_on"].append("monitor")
            else:
                template["services"][service_name]["depends_on"]["monitor"] = {
                    "condition": "service_started"
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
