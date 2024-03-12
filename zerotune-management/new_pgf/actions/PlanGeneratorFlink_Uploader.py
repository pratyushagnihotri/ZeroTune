import time
from utils.Ansible_Runner_Helper import run_playbook

class PlanGeneratorFlink_Runner:
    def __init__(self) -> None:
        print("Uploading PlanGeneratorFlink to cluster...")
        time.sleep(0.5)
        run_playbook("75-install-pgf.yml")
        run_playbook("76-run-flink.yml")
        input("PlanGeneratorFlink is uploaded and cluster is restarted. Enter to continue...")