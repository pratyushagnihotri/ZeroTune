import ansible_runner
import time
from utils.PGF_Parameters import get_parameter
from utils.Ansible_Inventory_Generator import generate_inventory


def run_playbook(playbook_filename):
    generate_inventory()
    inv = str(get_parameter("ansible_inventory"))
    playbooks_path = str(get_parameter("main_directory") +
                         "/zerotune-management/new_pgf/ansible_playbooks/")
    r = ansible_runner.run(
        inventory=inv, playbook=playbooks_path + playbook_filename)
    print("{}: {}".format(r.status, r.rc))
    if(r.rc != 0):
        for each_host_event in r.events:
            print(each_host_event['event'])
        print("Final status:")
        print(r.stats)
        raise RuntimeError("Ansible playbook run failed: " + r.status)
