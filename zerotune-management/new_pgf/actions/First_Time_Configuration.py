import time
from actions.Parameter_Manager import Parameter_Manager
from utils.Console_Utils import bcolors, clear_console

class First_Time_Configuration:

    def __init__(self) -> None:
        print("Setting up PlanGeneratorFlink for the first time...")
        time.sleep(1)
        self.configure()

    def configure(self):
        pm = Parameter_Manager("directMode")
        print(bcolors.WARNING +
              "If you already defined a value, press enter to leave it like it is.\n\n" + bcolors.ENDC)
        print("The setup assumes, that all necessary repositories are cloned in the same folder. It is recommended to place a folder called `ZeroTune` into your home directory. Inside this folder you need the repositories `zerotune-management`, `flink-observation`, `zerotune-plan-generation` and `zerotune-learning`. You need to take care to select the correct branch.")
        print("Take care, that the correct repositories with the right branch exists.")
        clear_console()
        pm.ask_for_main_directory()
        clear_console()
        print("Now face the remote cluster configuration.\n")
        pm.ask_for_ssh_key()
        clear_console()
        print("Now lets configure the docker registry.\n")
        pm.ask_for_docker_registry_credentials()
        clear_console()
        pm.ask_for_docker_image_name()
        clear_console()
        print(bcolors.OKGREEN +
              "That's it. Thanks, you will now be taken back to the menu." + bcolors.ENDC)
        time.sleep(4)