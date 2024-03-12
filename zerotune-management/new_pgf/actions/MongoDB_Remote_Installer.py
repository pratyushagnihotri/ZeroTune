from utils.Ansible_Runner_Helper import run_playbook
import time

class Flink_Docker_Image_Builder:

    def __init__(self) -> None:
        run_playbook("60-install-mongoDB.yml")
        input("MongoDeb setup done on remote cluster. Enter to continue...")