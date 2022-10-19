import subprocess

DEFAULT_BLOCK = "default"


def build_image(docker_image_name: str) -> str:
    subprocess.run(f"docker build -t {docker_image_name} .", shell=True)
    out = subprocess.run(
        f"docker images --no-trunc --quiet {docker_image_name}",
        shell=True,
        capture_output=True,
    )
    return out.stdout.decode().replace("\n", "")  # image sha


def save_block(block_obj, name: str = DEFAULT_BLOCK) -> None:
    uuid = block_obj.save(name, overwrite=True)
    slug = block_obj.dict().get("block_type_slug")
    print(f"Created block {slug}/{name} with ID: {uuid}")


def bash(command: str):
    subprocess.run(command, shell=True)
