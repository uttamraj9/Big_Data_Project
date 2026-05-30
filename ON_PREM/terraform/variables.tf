variable "cloudera_host" {
  description = "Cloudera server IP (SSH target for Python exec)"
  type        = string
  default     = "13.41.167.97"
}

variable "consultant_ssh_pass" {
  description = "SSH password for consultant user on Cloudera server"
  type        = string
  sensitive   = true
  default     = "WelcomeItc@2026"
}
