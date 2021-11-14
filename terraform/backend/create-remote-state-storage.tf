terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=2.46.0"
    }
  }
}

provider "azurerm" {
  features {}
}


# Configure the Microsoft Azure infrastructure for backend
resource "azurerm_resource_group" "tfstate" {
  name     = "terraform-group"
  location = "East US"
}

resource "azurerm_storage_account" "tfstate" {
  name                     = "terraformstatestorage420"
  resource_group_name      = azurerm_resource_group.tfstate.name
  location                 = azurerm_resource_group.tfstate.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  allow_blob_public_access = true
}

resource "azurerm_storage_container" "tfstate" {
  name                  = "terraform-state-container"
  storage_account_name  = azurerm_storage_account.tfstate.name
  container_access_type = "blob"
}