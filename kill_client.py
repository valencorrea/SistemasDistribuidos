import docker
import sys
import os
import signal

def main():
    if len(sys.argv) < 2:
        print("Usage: python kill_client.py <container_name>")
        sys.exit(1)

    container_name = sys.argv[1]
    os.environ['DOCKER_HOST'] = 'unix:///home/salvador/.docker/desktop/docker.sock'

    client = docker.from_env()
    try:
        container = client.containers.get(container_name)
        print(f"Killing container: {container.name}")
        container.kill(signal=signal.SIGKILL)
        print("Container killed successfully.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()