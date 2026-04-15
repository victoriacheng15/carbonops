# tofu init
# tofu plan
# tofu apply
# az aks get-credentials --resource-group rg-carbonops-aks --name aks-carbonops --file ./kubeconfig-aks-carbonops
# KUBECONFIG=./kubeconfig-aks-carbonops kubectl get nodes
# KUBECONFIG=./kubeconfig-aks-carbonops kubectl top pods -A
# KUBECONFIG=./kubeconfig-aks-carbonops cargo run -- detect
# KUBECONFIG=./kubeconfig-aks-carbonops cargo run -- collect
# KUBECONFIG=./kubeconfig-aks-carbonops cargo run -- collect --namespace kube-system

variable "location" {
  description = "The Azure region where resources will be created."
  type        = string
  default     = "Canada Central"
}

variable "resource_group_name" {
  description = "The name of the resource group."
  type        = string
  default     = "rg-carbonops-aks"
}

variable "cluster_name" {
  description = "The name of the AKS cluster."
  type        = string
  default     = "aks-carbonops"
}

variable "dns_prefix" {
  description = "The DNS prefix for the cluster."
  type        = string
  default     = "carbonops"
}

variable "node_count" {
  description = "The number of nodes in the default node pool."
  type        = number
  default     = 2
}

variable "vm_size" {
  description = "The size of the virtual machine for the nodes."
  type        = string
  default     = "Standard_D2s_v3"
}

output "client_certificate" {
  value     = azurerm_kubernetes_cluster.aks.kube_config[0].client_certificate
  sensitive = true
}

output "kube_config" {
  value     = azurerm_kubernetes_cluster.aks.kube_config_raw
  sensitive = true
}

output "cluster_name" {
  value = azurerm_kubernetes_cluster.aks.name
}

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "aks" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_kubernetes_cluster" "aks" {
  name                = var.cluster_name
  location            = azurerm_resource_group.aks.location
  resource_group_name = azurerm_resource_group.aks.name
  dns_prefix          = var.dns_prefix
  sku_tier            = "Free" # Explicitly use the Free Tier (no Uptime SLA cost)

  default_node_pool {
    name            = "default"
    node_count      = var.node_count
    vm_size         = var.vm_size
    os_disk_size_gb = 30 # Small disk to stay within free tier limits
  }

  identity {
    type = "SystemAssigned"
  }

  tags = {
    Environment = "Development"
  }
}
