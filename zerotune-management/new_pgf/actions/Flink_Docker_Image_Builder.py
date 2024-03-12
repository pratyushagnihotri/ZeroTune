from utils.Ansible_Runner_Helper import run_playbook
from utils.PGF_Parameters import store_parameter
import time
# from utils.PGF_Parameters import PGF_Parameters

class Flink_Docker_Image_Builder:

    def __init__(self) -> None:
        docker_image_tag = time.strftime("%Y%m%d-%H%M%S")
        store_parameter('docker_image_tag', docker_image_tag)
        run_playbook("09-build-and-push-flink-docker-image.yml")
        input("Docker image build and push is done. Enter to continue...")