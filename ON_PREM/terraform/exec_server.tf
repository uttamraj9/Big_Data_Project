# ─── SSH credentials for Cloudera exec ──────────────────────────────
resource "kubernetes_secret" "exec_server" {
  metadata {
    name      = "exec-server-secret"
    namespace = kubernetes_namespace.itc_training.metadata[0].name
    labels    = { app = "exec-server", "managed-by" = "terraform" }
  }
  data = {
    ssh-pass = var.consultant_ssh_pass
  }
}

# ─── Deployment ──────────────────────────────────────────────────────
resource "kubernetes_deployment" "exec_server" {
  metadata {
    name      = "exec-server"
    namespace = kubernetes_namespace.itc_training.metadata[0].name
    labels    = { app = "exec-server", "managed-by" = "terraform" }
  }

  spec {
    replicas = 1

    selector {
      match_labels = { app = "exec-server" }
    }

    template {
      metadata {
        labels = { app = "exec-server" }
      }

      spec {
        container {
          name  = "exec-server"
          image = "localhost:5000/exec-server:latest"

          port {
            container_port = 8891
            protocol       = "TCP"
          }

          env {
            name  = "SSH_HOST"
            value = var.cloudera_host
          }
          env {
            name  = "SSH_USER"
            value = "consultant"
          }
          env {
            name = "SSH_PASS"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.exec_server.metadata[0].name
                key  = "ssh-pass"
              }
            }
          }
          env {
            name  = "PYTHON_BIN"
            value = "/home/consultant/pyenv/bin/python"
          }
          env {
            name  = "HOME_DIR"
            value = "/home/consultant"
          }

          resources {
            requests = {
              cpu    = "50m"
              memory = "64Mi"
            }
            limits = {
              cpu    = "200m"
              memory = "128Mi"
            }
          }

          liveness_probe {
            http_get {
              path = "/health"
              port = 8891
            }
            initial_delay_seconds = 10
            period_seconds        = 30
            failure_threshold     = 3
          }

          readiness_probe {
            http_get {
              path = "/health"
              port = 8891
            }
            initial_delay_seconds = 5
            period_seconds        = 10
          }
        }
      }
    }
  }
}

# ─── Service (ClusterIP — reachable only inside the cluster) ─────────
resource "kubernetes_service" "exec_server" {
  metadata {
    name      = "exec-server"
    namespace = kubernetes_namespace.itc_training.metadata[0].name
    labels    = { app = "exec-server", "managed-by" = "terraform" }
  }

  spec {
    type     = "ClusterIP"
    selector = { app = "exec-server" }

    port {
      name        = "http"
      port        = 8891
      target_port = 8891
      protocol    = "TCP"
    }
  }
}
