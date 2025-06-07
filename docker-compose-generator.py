import sys

import yaml


def generate_docker_yaml(config):
    workers = config["workers"]
    clients = config["clients"]
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
            "environment": ["PYTHONUNBUFFERED=1"],
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
            "environment": ["PYTHONUNBUFFERED=1"]
        }

    # Crear 3 instancias de monitor cluster
    monitor_cluster_nodes = ["monitor_1", "monitor_2", "monitor_3"]
    monitor_service_ports = []
    
    for i in range(1, 4):
        monitor_name = f"monitor_{i}"
        service_port = 50000 + i  # 50001, 50002, 50003
        cluster_port = 50010 + i  # 50011, 50012, 50013
        monitor_service_ports.append(str(service_port))
        
        template["services"][monitor_name] = {
            "build": {
                "context": ".",
                "dockerfile": "monitor/monitor_cluster.dockerfile"
            },
            "ports": [
                f"{service_port}:{service_port}",  # Puerto para servicios
                f"{cluster_port}:{cluster_port}"   # Puerto para comunicación cluster
            ],
            "environment": [
                "PYTHONUNBUFFERED=1",
                f"MONITOR_SERVICE_PORT={service_port}",
                f"MONITOR_CLUSTER_PORT={cluster_port}", 
                "HEARTBEAT_INTERVAL=5000",
                "HEARTBEAT_TIMEOUT=15000",
                f"MONITOR_NODE_ID={i}",
                f"MONITOR_CLUSTER_NODES={','.join(monitor_cluster_nodes)}"
            ],
            "volumes": ["/var/run/docker.sock:/var/run/docker.sock"]
        }
    
    # Configurar dependencias de servicios a los monitores
    for service_name in template["services"]:
        if not service_name.startswith("monitor_") and service_name != "rabbitmq":
            if "environment" not in template["services"][service_name]:
                template["services"][service_name]["environment"] = []
            
            # Configurar múltiples monitores con sus puertos específicos
            template["services"][service_name]["environment"].extend([
                "MONITOR_HOSTS=monitor_1,monitor_2,monitor_3",
                f"MONITOR_PORTS={','.join(monitor_service_ports)}",
                "HEARTBEAT_INTERVAL=5000",
                f"SERVICE_NAME={service_name}"
            ])
            
            if "depends_on" not in template["services"][service_name]:
                template["services"][service_name]["depends_on"] = {}

            if isinstance(template["services"][service_name]["depends_on"], list):
                template["services"][service_name]["depends_on"].extend(["monitor_1", "monitor_2", "monitor_3"])
            else:
                for monitor_name in monitor_cluster_nodes:
                    template["services"][service_name]["depends_on"][monitor_name] = {
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
