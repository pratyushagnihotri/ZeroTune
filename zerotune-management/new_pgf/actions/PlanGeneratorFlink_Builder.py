import ansible_runner
import time
from utils.Ansible_Runner_Helper import run_playbook

class PlanGeneratorFlink_Builder:
    # pgf_parameters = PGF_Parameters()

    def __init__(self) -> None:
        print("Building PlanGeneratorFlink...")
        time.sleep(0.5)
        run_playbook("02-build-pgf.yml")
        input("PlanGeneratorFlink build is done. Enter to continue...")
        