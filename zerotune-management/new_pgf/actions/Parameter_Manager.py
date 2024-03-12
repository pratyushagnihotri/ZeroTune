import time
from utils.PGF_Parameters import store_parameter, get_parameter, get_all_parameters
from utils.Rspec_Parser import ask_for_rspec
import pathlib
import pprint

class Parameter_Manager:

    def __init__(self, mode) -> None:
        if mode == "view":
            self.view_parameters()
        elif mode == "change":
            self.change_parameters()
        elif mode == "directMode":
            pass

    def view_parameters(self):
        params = get_all_parameters()
        pprint.pprint(params, compact=True)
        #print(json.dumps(params, sort_keys=True, indent=4))
        input("\nPress Enter to continue...")

    def change_parameters(self):
        print("What parameter do you want to change?")
        choice = input(
            "a) Nodes b) Docker Image Name c) SSH Key filename d) Main directory e) Docker Registry Credentials f) Flink-Docker branch name: ")
        if choice == "a":
            ask_for_rspec()
        elif choice == "b":
            self.ask_for_docker_image_name()
        elif choice == "c":
            self.ask_for_ssh_key()
        elif choice == "d":
            self.ask_for_main_directory()
        elif choice == "e":
            self.ask_for_docker_registry_credentials()
        elif choice == "f":
            self.ask_for_flink_docker_branch_name()

    def ask_for_main_directory(self):
        main_directory = get_parameter('main_directory')
        if not main_directory:
            main_directory = pathlib.Path().resolve().parent.parent
            store_parameter('main_directory', main_directory)
        main_directory = input(
            'What is the absolute path of the main directory? (e.g. "/home/yourUserName/ZeroTune", currently: '+str(main_directory)+')\n')
        if main_directory:
            store_parameter('main_directory', main_directory)

    def ask_for_ssh_key(self):
        print("To be able to connect to the remote cluster, you need to add your ssh public key to the repository with which you can connect to the remote nodes.\nNo passphrase should be used. Put it into the main folder (zerotune-management) and make sure, that it has the file permissions 600.")
        ssh_key_filename = get_parameter('ssh_key_filename')
        ssh_key_filename = input(
            'What is its filename? (e.g. "geni.key", currently: '+str(ssh_key_filename)+')\n')
        if ssh_key_filename:
            store_parameter('ssh_key_filename', ssh_key_filename)

    def ask_for_docker_registry_credentials(self):
        print("The kubernetes nodes pull Apache Flink as a docker container. Therefore, the custom Apache Flink implementation needs to be stored online in a docker registry. You can use hub.docker.com")
        registry_url = get_parameter('registry_url')
        if not registry_url:
            registry_url = "hub.docker.com"
            store_parameter('registry_url', registry_url)
        registry_url = input(
            'What is your docker registry url? (e.g. "hub.docker.com", currently: '+str(registry_url)+')\n')
        if registry_url:
            store_parameter('registry_url', registry_url)

        registry_username = get_parameter('registry_username')
        registry_username = input(
            'What is your docker registry username? (e.g. "yourDockerRegistryUsername", currently: '+str(registry_username)+')\n')
        if registry_username:
            store_parameter('registry_username', registry_username)

        registry_read_write_key = get_parameter(
            'registry_read_write_key')
        registry_read_write_key = input(
            'What is your docker registry read and write key? It will not leave the device but is not stored securely. (e.g. "dckr_pat_aG8afz[...]", currently: '+str(registry_read_write_key)+')\n')
        if registry_read_write_key:
            store_parameter(
                'registry_read_write_key', registry_read_write_key)

        registry_read_only_key = get_parameter(
            'registry_read_only_key')
        registry_read_only_key = input(
            'What is your docker registry read only key? It is not stored securely and sent to the remote cluster. (e.g. "dckr_pat_aG8afz[...]", currently: '+str(registry_read_only_key)+')\n')
        if registry_read_only_key:
            store_parameter(
                'registry_read_only_key', registry_read_only_key)

    def ask_for_docker_image_name(self):
        docker_image_name = get_parameter('docker_image_name')
        docker_image_name = input(
            'What is the name for the docker image that should be used? This is up to you but should match with an existing repository in your account. (e.g. "custom-flink", currently: '+str(docker_image_name)+')\n')
        if docker_image_name:
            store_parameter('docker_image_name', docker_image_name)

    def ask_for_flink_docker_branch_name(self):
        print("To create a docker image out of the custom compiled Apache Flink code, the Dockerfiles of the corresponding base version of Flink are used. For this, the setup script needs to know what the branchname to the associated Flink base version is.")
        flink_docker_branch_name = get_parameter(
            'flink_docker_branch_name')
        if not flink_docker_branch_name:
            flink_docker_branch_name = "dev-1.16"
            store_parameter(
                'docker_image_name', flink_docker_branch_name)
        flink_docker_branch_name = input(
            'What is the name of the branch in Flink-Docker (https://github.com/apache/flink-docker) to the Flink Version that is used as a baseline? This is up to you. (e.g. "dev-1.16", currently: '+str(flink_docker_branch_name)+')\n')
        if flink_docker_branch_name:
            store_parameter(
                'docker_image_name', flink_docker_branch_name)
