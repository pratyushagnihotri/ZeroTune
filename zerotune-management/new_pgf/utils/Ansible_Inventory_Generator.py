from utils.PGF_Parameters import get_parameter, store_parameter
import base64


def getHostDict(node, ssh_key_path):
    return {
        node.get("client_id"): {
            "ansible_user": node.get("login_username"),
            "ansible_ssh_private_key_file": ssh_key_path,
            "ansible_host": node.get("login_hostname"),
            "alternative_hostname": node.get("alternative_hostname")
        }
    }


def generate_inventory():
    nodes = get_parameter("nodes")
    main_directory = get_parameter("main_directory")
    ssh_key_filename = get_parameter("ssh_key_filename")
    ssh_key_path = str(get_parameter("main_directory") +
                       "/zerotune-management/new_pgf/" + ssh_key_filename)
    registry_url = get_parameter("registry_url")
    registry_username = get_parameter("registry_username")
    registry_read_only_key = get_parameter("registry_read_only_key")
    registry_read_write_key = get_parameter("registry_read_write_key")
    docker_image_name = get_parameter("docker_image_name")
    flink_docker_branch_name = get_parameter("flink_docker_branch_name")
    pgf_runtime_parameter = get_parameter("pgf_runtime_parameter")
    flink_build_arguments = get_parameter("flink_build_arguments")
    adapt_flink_for_local_usage = get_parameter("adapt_flink_for_local_usage")
    docker_image_tag = get_parameter("docker_image_tag")

    inventory = {}
    if len(nodes) < 2:
        raise AttributeError("Not enough nodes to setup inventory.") #Todo: Maybe remove this to be able to build without remote cluster nodes

    vars = {
        #"dockerFlinkVersion": get_parameter("docker_flink_version"),
        "regcred": base64.b64encode(str({
            "auths": {
                registry_url: {
                    "username": registry_username,
                    # "password": password,
                    # "email": email,
                    "auth": registry_read_only_key
                }
            }
        }).encode('ascii')),
        "local_main_directory": main_directory,
        "registry_username": registry_username,
        "docker_image_name": docker_image_name,
        "flink_docker_branch_name": flink_docker_branch_name,
        "pgf_runtime_parameter": pgf_runtime_parameter,
        "flink_build_arguments": flink_build_arguments,
        "adapt_flink_for_local_usage": adapt_flink_for_local_usage
    }

    inventory.update({
        "all": {
            "vars": vars
        }
    })

    inventory.update({
        "masters": {
            "hosts": getHostDict(nodes[0], ssh_key_path)
        }
    })

    inventory.setdefault("workers", {}).update({
        "children": {
            "management": {
                "hosts": getHostDict(nodes[1], ssh_key_path),
            },
            "taskmanager": {
                "hosts": {
                    node.get("client_id"): {
                    "ansible_user": node.get("login_username"),
                    "ansible_ssh_private_key_file": ssh_key_path,
                    "ansible_host": node.get("login_hostname"),
                    "alternative_hostname": node.get("alternative_hostname")
                } for node in nodes[2:]}
            }
        }
    })
    store_parameter("ansible_inventory", inventory)
