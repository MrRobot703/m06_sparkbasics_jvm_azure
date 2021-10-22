# Spark application on aks cluster with terraform IaC tool

This a simple ETL spark application and is available on https://github.com/MrRobot703/m06_sparkbasics_jvm_azure. First there are some necessary components that
need to be installed and provisioned:

* Azure account
* Terraform
* Docker
* Maven
* Spark
* Auzer-cli

## Terraform
To get started just run:
```
terraform init
terraform apply
```
in the terraform folder. Also, if you want to keep .trstate file somewhere private,
I suggest you to check out [terraform official guids](https://www.terraform.io/docs/language/settings/backends/azurerm.html) 
for reference. The easiest so far approach is to authenticate via access_key with azure blob storage
and keep state file there.
And don't forget to run:
```
terraform destroy
```
after you finished with ETL job so not to waste resource in the cloud.

## Azure cli and Kubernetes
You can download azure cli with homebrew if you are on mac. For other platforms please refer to
[microsoft guids](https://docs.microsoft.com/en-us/cli/azure/)

```
brew update && brew install azure-cli
az login
```
After this you may want to install kubectl:
```
az aks install-cli
```
And then run:
```
az aks get-credentials --resource-group myResourceGroup --name myAKSCluster
```
for connecting to your aks cluster in azure, where myResourceGroup and myAKSCluster are yours
resource group name and cluster name respectively. If you want to know more about details and how
to set you aks cluster, I suggest you to follow this [guid](https://docs.microsoft.com/en-us/azure/aks/kubernetes-walkthrough)

## Docker and Maven

The Docker images is available on Docker Hub. To pull the image, run:
```
docker pull bigbackclock/sparkbasics:1.0.0 
```
To build an image for you own docker repo you can just run maven command:
```
maven istall
```

## Start the application
To do so you may run spark-submit.sh script. Just change master url to your 
aks api server url.
