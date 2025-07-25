import docker
import signal
import sys
import yaml
import os
import time
import random

def build_whitelist(config):
    whitelist = set()

    for name, data in config.get("workers", {}).items():
        if data.get("kill", False):
            for i in range(1, data.get("count", 0) + 1):
                if name == "credits" or name == "ratings":
                    whitelist.add(f"{name}_joiner_{i}")
                else:
                    whitelist.add(f"{name}_filter_{i}")

    for name, data in config.get("aggregators", {}).items():
        if data.get("kill", False):
            whitelist.add(f"{name}_aggregator")

    monitors_cfg = config.get("monitors", {})
    count = monitors_cfg.get("count", 1)
    kill_monitors = monitors_cfg.get("kill", False)

    if kill_monitors:
        for i in range(2, count + 1):
            whitelist.add(f"monitor_{i}")

    if config["client_decodifier"].get("kill", False):
        whitelist.add("client_decodifier")

    return whitelist

def main():
    os.environ['DOCKER_HOST'] = f'unix://{os.path.expanduser("~")}/.docker/desktop/docker.sock'
    with open(sys.argv[1], "r") as f:
        config = yaml.safe_load(f)

    whitelist = build_whitelist(config)
    random_opt = config["test"].get("random", False)
    interval = config["test"].get("interval", 10)

    print("Whitelist de contenedores:", whitelist)
    client = docker.from_env()
    def get_matching_containers():
        containers = client.containers.list(filters={"status": "running"})
        return [c for c in containers if c.name in whitelist]

    if random_opt:
        print("Random mode enabled. Killing containers randomly.")
        while True:
            try:
                matching = get_matching_containers()
                if not matching:
                    print("No matching containers found.")
                    time.sleep(interval)
                    continue
                container = random.choice(matching)
                print(f"Killing container: {container.name}")
                container.kill(signal=signal.SIGKILL)
                time.sleep(interval)
            except Exception as e:
                print(f"Error: {e}")
    else:
        matching = get_matching_containers()
        print(f"Killing selected containers: {[c.name for c in matching]} every {interval} seconds.")
        for container in matching:
            try:
                print(f"Killing container: {container.name}")
                container.kill(signal=signal.SIGKILL)
                time.sleep(interval)
            except Exception as e:
                print(f"Error killing {container.name}: {e}")

if __name__ == "__main__":
    main()