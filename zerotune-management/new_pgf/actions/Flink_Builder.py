import time
from utils.Ansible_Runner_Helper import run_playbook
from utils.PGF_Parameters import store_parameter
class Flink_Builder:

    def __init__(self, fast_build = False) -> None:
        print("Building Flink...")
        time.sleep(0.5)
        if fast_build:
            store_parameter('flink_build_arguments', ' -P docs-and-source -pl flink-streaming-java,flink-clients,flink-dist')
        else:
            store_parameter('flink_build_arguments', '')
        print("fast_build: " + str(fast_build))
        run_playbook("01-build-flink.yml")
        input("Flink Build is done. Enter to continue...")