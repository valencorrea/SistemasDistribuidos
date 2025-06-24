import docker
import signal
import sys
import yaml
import os
import time
import random

def main():

    os.environ['DOCKER_HOST'] = 'unix:///home/salvador/.docker/desktop/docker.sock'
    print(docker.__path__)

    with open(sys.argv[1], "r") as f:
        config = yaml.safe_load(f)

    whitelist = []
    random_opt = config["test"].get("random", False)
    interval = config["test"].get("interval", False)

    for name, data in config.get("workers", {}).items():
        if data.get("kill", False):
            whitelist.append(name)

    for name, data in config.get("aggregators", {}).items():
        if data.get("kill", False):
            whitelist.append(name)

    print("Whitelist of containers to kill:", whitelist)

    client = docker.from_env()

    if random_opt:
        print("Random mode enabled. Killing containers randomly.")
        while True:
            try:
                containers = client.containers.list(filters={"status": "running"})
                matching = [ c for c in containers if
                     any(c.name.startswith(f"dist-{w}_aggregator") or c.name.startswith(f"{w}_joiner") for w in whitelist) ]
                time.sleep(interval)
                if not matching:
                    print("No se encontraron contenedores para terminar.")
                    continue
                else:
                    print(f"Found matching containers: {matching}")
                container = random.choice(matching)
                print(f"SIGKILL container: {container.name}")
                container.kill(signal=signal.SIGKILL)
                matching.remove(container)
            except Exception as e:
                print(f"Error killing container: {e}")
    else:
        containers = client.containers.list(filters={"status": "running"})
        random.shuffle(containers)
        matching = [ c for c in containers if
                     any(c.name.startswith(f"dist-{w}_aggregator") or c.name.startswith(f"{w}_joiner") for w in whitelist) ]
        print(f"Killing selected containers: {matching} every {interval} seconds.")
        for container in matching:
            time.sleep(interval)
            print(f"SIGKILL container: {container.name}")
            container.kill(signal=signal.SIGKILL)


if __name__ == "__main__":
    main()