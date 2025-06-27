import sys

import yaml


def generate_docker_yaml(config):
    workers = config["workers"]
    clients = config["clients"]
    aggregators = config["aggregators"]
    
    monitors_config = config.get("monitors", {})
    monitor_count = monitors_config.get("count", 3)
    base_port = monitors_config.get("base_port", 50000)
    cluster_base_port = monitors_config.get("cluster_base_port", 50010)
    
    heartbeat_interval = monitors_config.get("heartbeat_interval", 5000)
    heartbeat_timeout = monitors_config.get("heartbeat_timeout", 15000)
    election_timeout = monitors_config.get("election_timeout", 10000)

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
                    "interval": "5s",
                    "timeout": "1s",
                    "retries": 50
                },
                "volumes": ["./rabbitmq/config.ini:/config.ini"]
            },
            "client_decodifier": {
                "build": {
                    "context": ".",
                    "dockerfile": "client_decodifier/client_decodifier.dockerfile",
                },
                "container_name": "client_decodifier",
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
                    "retries": 50
                }
            },
        }
    }

    all_services = ["worker", "client_decodifier"]

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
        all_services.append(name)

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
    credits_joiner_id_counter = 1
    ratings_joiner_id_counter = 1

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

        env = ["PYTHONUNBUFFERED=1", f"LOG_LEVEL={log_level}"]
        if "credits_joiner" in name:
            env.append(f"JOINER_INSTANCE_ID=credits-{credits_joiner_id_counter}")
            credits_joiner_id_counter += 1
            env.append("AGGREGATOR_HOST=top_10_credits_aggregator")
            env.append("AGGREGATOR_PORT=60000")
        elif "ratings_joiner" in name:
            env.append(f"JOINER_INSTANCE_ID=ratings-{ratings_joiner_id_counter}")
            ratings_joiner_id_counter += 1
            env.append("AGGREGATOR_HOST=best_and_worst_ratings_aggregator")
            env.append("AGGREGATOR_PORT=60002")

        base_service = {
            "build": {
                "context": ".",
                "dockerfile": dockerfile
            },
            "depends_on": {
                "rabbitmq": {"condition": "service_healthy"},
                "worker": {"condition": "service_started"}
            },
            "links": ["rabbitmq"],
            "environment": env,
            "image": f"{name}:latest",
            "container_name": f"{name}_1"
        }
        template["services"][name] = base_service
        all_services.append(f"{name}_1")

        for i in range(1, count):
            replica_name = f"{name}_{i + 1}"
            replica_env = ["PYTHONUNBUFFERED=1", f"LOG_LEVEL={log_level}"]
            
            if "credits_joiner" in name:
                replica_env.append(f"JOINER_INSTANCE_ID=credits-{credits_joiner_id_counter + i}")
                replica_env.append("AGGREGATOR_HOST=top_10_credits_aggregator")
                replica_env.append("AGGREGATOR_PORT=60000")
            elif "ratings_joiner" in name:
                replica_env.append(f"JOINER_INSTANCE_ID=ratings-{ratings_joiner_id_counter + i}")
                replica_env.append("AGGREGATOR_HOST=best_and_worst_ratings_aggregator")
                replica_env.append("AGGREGATOR_PORT=60002")
            
            template["services"][replica_name] = {
                "image": f"{name}:latest",
                "container_name": f"{replica_name}",
                "depends_on": {
                    "rabbitmq": {"condition": "service_healthy"},
                    "worker": {"condition": "service_started"}
                },
                "links": ["rabbitmq"],
                "environment": replica_env
            }
            all_services.append(replica_name)

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
        service_def = {
            "build": {
                "context": ".",
                "dockerfile": dockerfile
            },
            "container_name": name,
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

        if "credits" in name:
            service_def["ports"] = ["60000:60000"]
        elif "ratings" in name:
            service_def["ports"] = ["60002:60002"]

        template["services"][name] = service_def
        all_services.append(name)

    monitor_cluster_nodes = [f"monitor_{i}" for i in range(1, monitor_count + 1)]
    monitor_service_ports = []
    
    expected_services_str = ",".join(all_services)
    
    for i in range(1, monitor_count + 1):
        monitor_name = f"monitor_{i}"
        service_port = base_port + i
        cluster_port = cluster_base_port + i
        monitor_service_ports.append(str(service_port))
        
        template["services"][monitor_name] = {
            "build": {
                "context": ".",
                "dockerfile": "monitor/monitor_cluster.dockerfile"
            },
            "ports": [
                f"{service_port}:{service_port}",
                f"{cluster_port}:{cluster_port}"
            ],
            "container_name": monitor_name,
            "environment": [
                "PYTHONUNBUFFERED=1",
                f"MONITOR_SERVICE_PORT={service_port}",
                f"MONITOR_CLUSTER_PORT={cluster_port}", 
                f"HEARTBEAT_INTERVAL={heartbeat_interval}",
                f"HEARTBEAT_TIMEOUT={heartbeat_timeout}",
                f"ELECTION_TIMEOUT={election_timeout}",
                f"MONITOR_NODE_ID={i}",
                f"MONITOR_CLUSTER_NODES={','.join(monitor_cluster_nodes)}",
                f"EXPECTED_SERVICES={expected_services_str}"
            ],
            "volumes": ["/var/run/docker.sock:/var/run/docker.sock"]
        }
    
    for service_name in template["services"]:
        if not service_name.startswith("monitor_") and service_name != "rabbitmq":
            if "environment" not in template["services"][service_name]:
                template["services"][service_name]["environment"] = []
            
            container_name = template["services"][service_name].get("container_name", service_name)
            
            template["services"][service_name]["environment"].extend([
                f"MONITOR_HOSTS={','.join(monitor_cluster_nodes)}",
                f"MONITOR_PORTS={','.join(monitor_service_ports)}",
                f"HEARTBEAT_INTERVAL={heartbeat_interval}",
                f"SERVICE_NAME={container_name}"
            ])
            
            if "depends_on" not in template["services"][service_name]:
                template["services"][service_name]["depends_on"] = {}

            if isinstance(template["services"][service_name]["depends_on"], list):
                template["services"][service_name]["depends_on"].extend(monitor_cluster_nodes)
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
