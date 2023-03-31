from prefect.infrastructure.docker import DockerContainer, ImagePullPolicy
from 
docker_job = DockerContainer(
    image="ktncktnc/prefect_gharchive:latest",
    image_pull_policy=ImagePullPolicy.ALWAYS
)
uuid = docker_job.save("docker-gharchive", overwrite=True)
print(uuid)