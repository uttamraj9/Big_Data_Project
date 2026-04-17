terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.25"
    }
  }
  required_version = ">= 1.5.0"
}

provider "kubernetes" {
  config_path    = "~/.kube/config"
  config_context = "kubernetes-admin@kubernetes"
}

# ─── Namespace ───────────────────────────────────────────────
resource "kubernetes_namespace" "itc_training" {
  metadata {
    name = "itc-training"
    labels = {
      project    = "itc-training-portal"
      managed-by = "terraform"
    }
  }
}

# ─── ServiceAccount for training portal ─────────────────────
resource "kubernetes_service_account" "training_portal" {
  metadata {
    name      = "training-portal"
    namespace = kubernetes_namespace.itc_training.metadata[0].name
  }
}

# ─── Role — read-only view of pods in namespace ──────────────
resource "kubernetes_role" "training_viewer" {
  metadata {
    name      = "training-viewer"
    namespace = kubernetes_namespace.itc_training.metadata[0].name
  }
  rule {
    api_groups = [""]
    resources  = ["pods", "services", "endpoints"]
    verbs      = ["get", "list", "watch"]
  }
}

resource "kubernetes_role_binding" "training_viewer" {
  metadata {
    name      = "training-viewer-binding"
    namespace = kubernetes_namespace.itc_training.metadata[0].name
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.training_viewer.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.training_portal.metadata[0].name
    namespace = kubernetes_namespace.itc_training.metadata[0].name
  }
}
