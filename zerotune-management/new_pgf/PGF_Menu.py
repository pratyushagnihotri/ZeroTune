from consolemenu import *
from consolemenu.items import *
from consolemenu.format import *
from consolemenu.menu_component import Dimension
from actions.First_Time_Configuration import First_Time_Configuration
from actions.PlanGeneratorFlink_Builder import PlanGeneratorFlink_Builder
from actions.Flink_Builder import Flink_Builder
from actions.Remote_Kubernetes_Cluster_Setup import Remote_Kubernetes_Cluster_Setup
from actions.Parameter_Manager import Parameter_Manager
from actions.Local_Cluster_Setup import Local_Cluster_Setup
from actions.Flink_Docker_Image_Builder import Flink_Docker_Image_Builder
from actions.PlanGeneratorFlink_Uploader import PlanGeneratorFlink_Uploader
from actions.Flink_Uploader import Flink_Uploader
from actions.Flink_Remote_Runner import Flink_Remote_Runner
from actions.MongoDB_Remote_Installer import MongoDB_Remote_Installer

class PGF_Menu:
    def __init__(self) -> None:
        self.show_menu()

    def first_time_setup(self):
        pass
    
    def show_menu(self):
        menu = ConsoleMenu("> PlanGeneratorFlink Main Menu <", prologue_text="""
        ▛▀▖▜       ▞▀▖               ▐        ▛▀▘▜ ▗    ▌ _
        ▙▄▘▐ ▝▀▖▛▀▖▌▄▖▞▀▖▛▀▖▞▀▖▙▀▖▝▀▖▜▀ ▞▀▖▙▀▖▙▄ ▐ ▄ ▛▀▖▌▗▘
        ▌  ▐ ▞▀▌▌ ▌▌ ▌▛▀ ▌ ▌▛▀ ▌  ▞▀▌▐ ▖▌ ▌▌  ▌  ▐ ▐ ▌ ▌▛▚ 
        ▘   ▘▝▀▘▘ ▘▝▀ ▝▀▘▘ ▘▝▀▘▘  ▝▀▘ ▀ ▝▀ ▘  ▘   ▘▀▘▘ ▘▘ ▘
        This setup program will help you to set up, rebuild and apply PlanGeneratorFlink. 
        For help, check out the readme: https://github.com/pratyushagnihotri/ZeroTune""")

        main_menu_format = MenuFormatBuilder(max_dimension=Dimension(width=100, height=40))
        main_menu_format.set_title_align('center')
        main_menu_format.set_prologue_text_align('center')
        main_menu_format.show_prologue_bottom_border(True)
        menu.formatter = main_menu_format

        sub_menu_format = MenuFormatBuilder(max_dimension=Dimension(width=100, height=40))
        sub_menu_format.show_item_bottom_border("Remote kubernetes cluster", True)
        sub_menu_format.show_item_bottom_border("Collect results from remote cluster to local folder", True)
        sub_menu_format.show_item_bottom_border("Build with Kubernetes cluster config", True)
        sub_menu_format.show_item_bottom_border("Run on Kubernetes cluster", True)
        sub_menu_format.show_item_bottom_border("Stop", True)
        sub_menu_format.show_item_bottom_border("Rebuild only relevant modules (fast rebuild)", True)
        sub_menu_format.show_item_bottom_border("Run on remote Kubernetes cluster", True)
        sub_menu_format.show_item_bottom_border("Restart on remote Kubernetes cluster", True)
        sub_menu_format.show_item_bottom_border("Stop on remote Kubernetes cluster", True)
        sub_menu_format.show_item_bottom_border("Upload Docker image to registry", True)
        sub_menu_format.show_item_bottom_border("Clean on remote Kubernetes cluster", True)
        sub_menu_format.show_item_bottom_border("Clean MongoDB", True)
        sub_menu_format.show_item_bottom_border("Print remote Kubernetes cluster dashboard token", True)

        # ########################### #
        # #### - Configuration - #### #
        # ########################### #
        configuration_menu = ConsoleMenu("Configuration", formatter=sub_menu_format, prologue_text="Configure all parameters like credentials, remote nodes or directory names.")
        menu.append_item(SubmenuItem("Configuration", configuration_menu, menu))
        configuration_menu.append_item(FunctionItem("First time configuration", First_Time_Configuration))
        configuration_menu.append_item(FunctionItem("View parameters", Parameter_Manager, ["view"]))
        configuration_menu.append_item(FunctionItem("Change parameters", Parameter_Manager, ["change"]))

        # ########################### #
        # #### - Setup cluster - #### #
        # ########################### #
        setup_cluster_menu = ConsoleMenu("Setup cluster", formatter=sub_menu_format, prologue_text="This option helps you to easily set up a local or remote cluster. After setting up the cluster you can run PlanGeneratorFlink on it.")
        menu.append_item(SubmenuItem("Setup cluster", setup_cluster_menu, menu))
        main_menu_format.show_item_bottom_border("Setup cluster", True)
        setup_cluster_menu.append_item(FunctionItem("Local cluster", Local_Cluster_Setup))
        setup_cluster_menu.append_item(FunctionItem("Remote kubernetes cluster", Remote_Kubernetes_Cluster_Setup))

        # --------------------------------------------------------------------------------------------------------------

        # ################################ #
        # #### - PlanGeneratorFlink - #### #
        # ################################ #

        # store "--mode train -n 200 --parallelismStrategy INCREASING --deterministic" as pgf_runtime_parameter oder so

        pgf_menu = ConsoleMenu("PlanGeneratorFlink", formatter=sub_menu_format)
        menu.append_item(SubmenuItem("PlanGeneratorFlink", pgf_menu, menu))

        pgf_menu.append_item(FunctionItem("Build", PlanGeneratorFlink_Builder))
        pgf_menu.append_item(FunctionItem("Upload to remote cluster", PlanGeneratorFlink_Uploader))

        run_pgf_menu = ConsoleMenu("Run PlanGeneratorFlink", formatter=sub_menu_format)
        pgf_menu.append_item(SubmenuItem("Run", run_pgf_menu, menu))
        run_pgf_menu.append_item(FunctionItem("Run locally", self.first_time_setup))
        run_pgf_menu.append_item(FunctionItem("Run on Kubernetes cluster", self.first_time_setup))

        pgf_menu.append_item(FunctionItem("Collect results from remote cluster to local folder", self.first_time_setup))

        # ########################## #
        # #### - Apache Flink - #### #
        # ########################## #
        flink_menu = ConsoleMenu("Apache Flink", formatter=sub_menu_format)
        menu.append_item(SubmenuItem("Apache Flink", flink_menu, menu))

        build_flink_menu = ConsoleMenu("Build Apache Flink", formatter=sub_menu_format)
        flink_menu.append_item(SubmenuItem("Build", build_flink_menu, menu))
        build_flink_menu.append_item(FunctionItem("Build from scratch (full rebuild)", Flink_Builder, kwargs={"fast_build": False}))
        build_flink_menu.append_item(FunctionItem("Rebuild only relevant modules (fast rebuild)",  Flink_Builder, kwargs={"fast_build": True}))

        flink_menu.append_item(FunctionItem("Upload to remote cluster", Flink_Uploader))

        run_flink_menu = ConsoleMenu("(Re-)Run Apache Flink", formatter=sub_menu_format)
        flink_menu.append_item(SubmenuItem("(Re-)Run", run_flink_menu, menu))
        run_flink_menu.append_item(FunctionItem("(Re-)Run on local cluster", self.first_time_setup))
        run_flink_menu.append_item(FunctionItem("(Re-)Run on remote Kubernetes cluster", Flink_Remote_Runner))

        flink_menu.append_item(FunctionItem("Stop local Apache Flink cluster", self.first_time_setup))

        # ########################################## #
        # #### - Docker Image of Apache Flink - #### #
        # ########################################## #
        menu.append_item(FunctionItem("Build and push Docker image", Flink_Docker_Image_Builder))

        # ##################### #
        # #### - MongoDB - #### #
        # ##################### #
        mongoDB_menu = ConsoleMenu("MongoDB", formatter=sub_menu_format)
        menu.append_item(SubmenuItem("MongoDB", mongoDB_menu, menu))

        run_mongoDB_menu = ConsoleMenu("Run MongoDB", formatter=sub_menu_format)
        mongoDB_menu.append_item(SubmenuItem("Run MongoDB", run_mongoDB_menu, menu))
        run_mongoDB_menu.append_item(FunctionItem("Run on local cluster", self.first_time_setup))
        run_mongoDB_menu.append_item(FunctionItem("Run on remote Kubernetes cluster", MongoDB_Remote_Installer))

        clean_mongoDB_menu = ConsoleMenu("Clean MongoDB", formatter=sub_menu_format)
        mongoDB_menu.append_item(SubmenuItem("Clean MongoDB", clean_mongoDB_menu, menu))
        clean_mongoDB_menu.append_item(FunctionItem("Clean on local cluster", self.first_time_setup))
        clean_mongoDB_menu.append_item(FunctionItem("Clean on remote Kubernetes cluster", self.first_time_setup))

        # ######################## #
        # #### - Kubernetes - #### #
        # ######################## #
        kubernetes_menu = ConsoleMenu("Kubernetes", formatter=sub_menu_format)
        menu.append_item(SubmenuItem("Kubernetes", kubernetes_menu, menu))
        kubernetes_menu.append_item(FunctionItem("Setup Kubernetes on remote cluster", self.first_time_setup))
        kubernetes_menu.append_item(FunctionItem("Connect via ssh to remote cluster master node", self.first_time_setup))
        kubernetes_menu.append_item(FunctionItem("Portforward flink web gui from remote cluster", self.first_time_setup))
        kubernetes_menu.append_item(FunctionItem("Print remote Kubernetes cluster dashboard token", self.first_time_setup))

        # --------------------------------------------------------------------------------------------------------------

        # Finally, we call show to show the menu and allow the user to interact
        menu.show()
