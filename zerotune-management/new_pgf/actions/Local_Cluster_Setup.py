import time
import json
from utils.PGF_Parameters import store_parameter, get_parameter
from utils.Rspec_Parser import ask_for_rspec
from utils.Console_Utils import clear_console
from utils.Ansible_Runner_Helper import run_playbook


from actions.PlanGeneratorFlink_Builder import PlanGeneratorFlink_Builder
from actions.Flink_Builder import Flink_Builder
from actions.Flink_Docker_Image_Builder import Flink_Docker_Image_Builder


class Local_Cluster_Setup:

    def __init__(self) -> None:
        print("Setting up local cluster...")
        time.sleep(0.5)
        
        rebuild_pgf = False
        rebuild_flink = False
        yn = input('Do you want to (re-)build PlanGeneratorFlink? [y/n]').lower()
        if yn.startswith('y'):
                rebuild_pgf = True
        yn = input('Do you want to (re-)build Apache Flink? [y/n]').lower()
        if yn.startswith('y'):
                rebuild_flink = True
        
        if rebuild_pgf:
            PlanGeneratorFlink_Builder()
        if rebuild_flink:
            Flink_Builder(True)
        self.setup_local_cluster()
        input("Local cluster setup is done. If you want to run PlanGeneratorFlink now, select it in the menu. You can access the flink web ui here: http://localhost:8081")
        
        
    def setup_local_cluster(self):
        run_playbook("03-start-local-cluster.yml")