from utils.Ansible_Runner_Helper import run_playbook
import time

class Flink_Docker_Image_Builder:

    def __init__(self) -> None:
        run_playbook("70-install-flink-cluster.yml")
        run_playbook("75-install-pgf")
        input("Flink uploaded (incl. PGF) and started on remote cluster. Enter to continue...")