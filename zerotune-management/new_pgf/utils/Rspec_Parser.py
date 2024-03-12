import xml.etree.ElementTree as ET
import sys
import time
from utils.PGF_Parameters import store_parameter
from utils.Ansible_Inventory_Generator import generate_inventory
from utils.Console_Utils import bcolors

ns = {
        'geni': 'http://www.geni.net/resources/rspec/3',
        'emulab': 'http://www.protogeni.net/resources/rspec/ext/emulab/1'
    }

def parse_and_store_rspec(rspec):
    nodes = []
    try:
        xml_root = ET.fromstring(rspec)
        for xml_node in xml_root.iterfind('geni:node', ns):
            nodes.append({
                'client_id': xml_node.attrib.get("client_id"),
                'alternative_hostname': xml_node.find("geni:host", ns).attrib.get("name"),
                'login_hostname': xml_node.find("geni:services", ns).find("geni:login", ns).attrib.get("hostname"),
                'login_username': xml_node.find("geni:services", ns).find("geni:login", ns).attrib.get("username"),
                'hardware_type': xml_node.find("emulab:vnode", ns).attrib.get("hardware_type")
            })
    except:
        print("Rspec cannot be parsed.")
    else:
        store_parameter('nodes', nodes)
        generate_inventory()
        print(bcolors.OKGREEN + "\n\n\n Rspec successfully parsed and stored." +  bcolors.ENDC)
    time.sleep(2)

def ask_for_rspec():
    print('Please paste you rspec definition containing the considered nodes (confirm with ctrl+d): ')
    rspec = sys.stdin.read()
    parse_and_store_rspec(rspec)