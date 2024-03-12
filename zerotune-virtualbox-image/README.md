# ZeroTune - VM Image

This project include VM image of `ZeroTune` to provide user already setup environment of dependencies and code that is required for generating training data and training/inference of `ZeroTune` model.

## Table of Contents
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Project Structure](#project-structure)

## Getting Started

Explain how users can get started with `ZeroTune` VM using the provided OVA image and VirtualBox.

### Prerequisites

Before you begin, ensure you have the following software installed:

- [VirtualBox](https://www.virtualbox.org/)
- [Git](https://git-scm.com/)

### Installation

1. Download the OVA image of Ubuntu 20.04 Server from [here](https://drive.google.com/drive/folders/14J0eMXTXNryHxfQWf2Ia3KHRBwTC6A8s?usp=sharing).

2. Open VirtualBox.

3. Go to `File` > "`Import Appliance.`

4. In the Import Virtual Appliance window, click the "Open Appliance" button and select the downloaded OVA file.

5. Click `Next` to review the appliance settings and then click `Import.`

6. Once the import is complete, you can start the VM by selecting it in VirtualBox and clicking the "Start" button.

7. Log in to the VM using the following credentials:
   - Username: `zt`
   - Password: `zt`

### Accessing the VM

To access the VM easily, you can set up port forwarding in VirtualBox:

1. Select the VM in VirtualBox.

2. Go to `Settings` > `Network` > `Advanced` > `Port Forwarding.`

3. Add a rule to forward port 22 to whatever port you prefer for SSH access.

4. Optionally, add a forwarding rule for port `8081` to access the web frontend.

### Accessing the Repository

The project's repository can be found in the home directory of the VM:

```bash
~/dsps/ or ~/ZeroTune
```

### Project Structure

The key component of `ZeroTune` includes

- [zerotune-management:](https://github.com/pratyushagnihotri/ZeroTune/tree/master/zerotune-management#readme) The main instructions to setup is in zerotune-management. It consists collection of scripts that facilitate the seamless setup of both local and remote clusters. These clusters serve as the foundation for the parallel query plan generator and environment for zero-shot model for training and test purpose.

- [zerotune-plan-generator:](https://github.com/pratyushagnihotri/ZeroTune/tree/master/zerotune-plan-generation#readme) Apache flink client application which functions as an essential tool for generating synthetic and benchmark parallel query plans. These plans are vital for the training and testing of data, a crucial aspect of our zero-shot learning model.

- [zerotune-learning:](https://github.com/pratyushagnihotri/ZeroTune/tree/master/zerotune-learning/flink_learning#readme) zero-shot model that specializes in providing accurate cost predictions for distributed parallel stream processing.

- [Flink-Observation:](https://github.com/pratyushagnihotri/ZeroTune/tree/master/flink-observation#readme) Modified the fork of Apache Flink for custom logging of observation of workload characteristics and loggin them in MongoDB database.