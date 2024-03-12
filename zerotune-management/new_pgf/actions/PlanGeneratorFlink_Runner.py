import ansible_runner
import time
from utils.Ansible_Runner_Helper import run_playbook

class PlanGeneratorFlink_Runner:
    def __init__(self) -> None:
        print("Running PlanGeneratorFlink...")
        time.sleep(0.5)
        self.define_pgf_run_parameters()
        run_playbook("02-build-pgf.yml")
        input("PlanGeneratorFlink is running. Enter to continue...")
        
    def define_pgf_run_parameters(self):
        pass