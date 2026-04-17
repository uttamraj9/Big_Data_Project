#!/usr/bin/env bash
set -euo pipefail

# ─── Config ────────────────────────────────────────────────────
SERVER="13.42.152.118"
PEM="/Users/uttamkumar/Desktop/Training/test_key.pem"
SSH="ssh -i $PEM -o StrictHostKeyChecking=no ec2-user@$SERVER"
SCP="scp -i $PEM -o StrictHostKeyChecking=no"
PORTAL_SRC="/Users/uttamkumar/Downloads/itc-training-portal"
REGISTRY="localhost:5000"
IMAGE="itc-training-portal"
TAG="latest"

log()  { echo -e "\033[1;34m[$(date '+%H:%M:%S')] $*\033[0m"; }
ok()   { echo -e "\033[1;32m  ✓ $*\033[0m"; }
err()  { echo -e "\033[1;31m  ✗ $*\033[0m"; exit 1; }

# ─── Step 1: Start local Docker registry on server ─────────────
log "Step 1/6 — Ensuring local Docker registry"
$SSH "
  if ! docker ps | grep -q local-registry; then
    docker run -d -p 5000:5000 --restart=always --name local-registry \
      -v /var/lib/registry:/var/lib/registry registry:2
    echo 'Registry started'
  else
    echo 'Registry already running'
  fi
"
ok "Registry at $SERVER:5000"

# ─── Step 2: Build React app + Docker image on server ──────────
log "Step 2/6 — Syncing source & building Docker image"

# Copy portal source to server
$SSH "mkdir -p /opt/itc/build/training-portal"
rsync -az --exclude=node_modules --exclude=dist \
  -e "ssh -i $PEM -o StrictHostKeyChecking=no" \
  "$PORTAL_SRC/" "ec2-user@$SERVER:/opt/itc/build/training-portal/"

# Copy Dockerfile and nginx.conf from this repo
$SCP ON_PREM/docker/training-portal/Dockerfile \
  "ec2-user@$SERVER:/opt/itc/build/training-portal/Dockerfile"
$SCP ON_PREM/docker/training-portal/nginx.conf \
  "ec2-user@$SERVER:/opt/itc/build/training-portal/nginx.conf"

# Build and push to local registry
$SSH "
  cd /opt/itc/build/training-portal
  docker build -t $REGISTRY/$IMAGE:$TAG . 2>&1 | tail -5
  docker push $REGISTRY/$IMAGE:$TAG
"
ok "Image built and pushed to $REGISTRY/$IMAGE:$TAG"

# ─── Step 3: Copy K8s manifests ────────────────────────────────
log "Step 3/6 — Uploading K8s manifests"
$SSH "mkdir -p /opt/itc/k8s/training-portal /opt/itc/k8s/lab-proxy"
$SCP ON_PREM/k8s/namespace.yaml        "ec2-user@$SERVER:/opt/itc/k8s/"
$SCP ON_PREM/k8s/training-portal/*.yaml "ec2-user@$SERVER:/opt/itc/k8s/training-portal/"
$SCP ON_PREM/k8s/lab-proxy/*.yaml      "ec2-user@$SERVER:/opt/itc/k8s/lab-proxy/"
ok "Manifests uploaded"

# ─── Step 4: Apply K8s manifests ───────────────────────────────
log "Step 4/6 — Applying K8s manifests"
$SSH "
  kubectl apply -f /opt/itc/k8s/namespace.yaml
  kubectl apply -f /opt/itc/k8s/training-portal/
  kubectl apply -f /opt/itc/k8s/lab-proxy/

  echo '--- Waiting for training-portal rollout ---'
  kubectl rollout status deployment/training-portal -n itc-training --timeout=120s

  echo '--- Waiting for lab-proxy rollout ---'
  kubectl rollout status deployment/lab-proxy -n itc-training --timeout=120s
"
ok "K8s pods running"

# ─── Step 5: Configure Apache proxy (port 3000 → NodePort) ─────
log "Step 5/6 — Configuring Apache proxy"
$SSH "
  # Add Listen 3000 if not present
  grep -q 'Listen 3000' /etc/httpd/conf/httpd.conf || \
    echo 'Listen 3000' >> /etc/httpd/conf/httpd.conf

  cat > /etc/httpd/conf.d/training-portal.conf << 'EOF'
<VirtualHost *:3000>
    ProxyPreserveHost On
    ProxyPass        / http://127.0.0.1:30300/
    ProxyPassReverse / http://127.0.0.1:30300/
    ErrorLog  /var/log/httpd/training-portal-error.log
    CustomLog /var/log/httpd/training-portal-access.log combined
</VirtualHost>
EOF

  systemctl reload httpd
"
ok "Apache proxy → port 3000"

# ─── Step 6: Print access URLs ─────────────────────────────────
log "Step 6/6 — Deployment complete"
echo ""
echo "  ╔══════════════════════════════════════════════════════════╗"
echo "  ║            ITC Training Portal — Access URLs            ║"
echo "  ╠══════════════════════════════════════════════════════════╣"
echo "  ║  Training Portal    http://$SERVER:3000               ║"
echo "  ╠══════════════════════════════════════════════════════════╣"
echo "  ║  Hadoop Lab UIs (via K8s lab-proxy)                     ║"
echo "  ║  YARN ResourceManager  http://$SERVER:30088           ║"
echo "  ║  HDFS NameNode         http://$SERVER:30870           ║"
echo "  ║  Spark History         http://$SERVER:31088           ║"
echo "  ║  Cloudera Manager      http://$SERVER:37180           ║"
echo "  ║  Hue                   http://$SERVER:30889           ║"
echo "  ╠══════════════════════════════════════════════════════════╣"
echo "  ║  Direct (if SG allows)                                  ║"
echo "  ║  Cloudera Manager      http://13.41.167.97:7180         ║"
echo "  ║  Hue                   http://13.41.167.97:8888         ║"
echo "  ║  Jenkins               http://$SERVER:8080            ║"
echo "  ╚══════════════════════════════════════════════════════════╝"
echo ""
echo "  NOTE: Open these ports in AWS Security Group if not yet done:"
echo "  Inbound: TCP 3000, 30088, 30870, 31088, 37180, 30889"

# Show pod status
$SSH "kubectl get pods,svc -n itc-training -o wide"
