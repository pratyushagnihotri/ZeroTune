import time
import sys
import json
from utils.PGF_Parameters import store_parameter, get_parameter
from utils.Rspec_Parser import ask_for_rspec
from utils.Console_Utils import clear_console
from utils.Ansible_Runner_Helper import run_playbook


from actions.PlanGeneratorFlink_Builder import PlanGeneratorFlink_Builder
from actions.Flink_Builder import Flink_Builder
from actions.Flink_Docker_Image_Builder import Flink_Docker_Image_Builder


class Remote_Kubernetes_Cluster_Setup:

    def __init__(self) -> None:
        print("Setting up Remote Kubernetes cluster...")
        time.sleep(0.5)
        nodes = get_parameter('nodes')
        if nodes:
            print("Currently stored nodes:")
            print(json.dumps(nodes, sort_keys=True, indent=4))
            yn = input('Do you want to parse a new cluster definition (else use the existing one)? [y/n]').lower()
            if yn.startswith('y'):
                ask_for_rspec()
        else:
            ask_for_rspec()
        time.sleep(0.5)
        clear_console()
        print("Now let's start setting up the cluster.")
        yn = input('Do you want to (re-)build PlanGeneratorFlink? [y/n]').lower()
        
        rebuild_pgf = False
        rebuild_flink = False
        if yn.startswith('y'):
                rebuild_pgf = True
        yn = input('Do you want to (re-)build Apache Flink (and therefore also a new docker image)? [y/n]').lower()
        if yn.startswith('y'):
                rebuild_flink = True
        
        if rebuild_pgf:
            PlanGeneratorFlink_Builder()
        if rebuild_flink:
            Flink_Builder(True)
            Flink_Docker_Image_Builder()
        self.setup_kubernetes()
        input("Kubernetes Cluster setup is done. If you want to run PlanGeneratorFlink now, select it in the menu.")
        
        
    def setup_kubernetes(self):
        run_playbook("07-flink-apply-remote-config.yml")
        run_playbook("10-install-k8s.yml")
        run_playbook("20-setup-master.yml")
        run_playbook("30-setup-workers.yml")
        run_playbook("40-setup-pgf.yml")
        run_playbook("50-install-k8s-dashboard.yml")
        run_playbook("60-install-mongoDB.yml")
        run_playbook("70-install-flink-cluster.yml")
        run_playbook("75-install-pgf.yml")
        run_playbook("76-run-flink.yml")
        
