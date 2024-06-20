# Create a VPC
resource "google_compute_network" "vpc_network" {
  depends_on                      = [google_project_service.enabled_apis]
  name                            = var.vpc_params.vpc_name
  delete_default_routes_on_create = false
  auto_create_subnetworks         = false
  routing_mode                    = "REGIONAL"
  project                         = var.common_params.project
}

resource "google_compute_subnetwork" "vpc_subnetwork" {
  depends_on               = [google_compute_network.vpc_network]
  name                     = var.vpc_params.subnetwork_name
  ip_cidr_range            = var.vpc_params.subnetwork_cidr_range
  private_ip_google_access = true
  region                   = var.common_params.region
  network                  = google_compute_network.vpc_network.id
  project                  = var.common_params.project
}


# Create compute engine
resource "google_compute_instance" "mysql_database_instance" {
  depends_on                = [google_compute_subnetwork.vpc_subnetwork]
  project                   = var.common_params.project
  name                      = var.mysql_params.vm_name
  machine_type              = var.mysql_params.machine_type
  zone                      = var.mysql_params.zone
  tags                      = ["databases"]
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      labels = {
        gcp-environment = "dev"
      }
    }
  }

  network_interface {
    network    = google_compute_network.vpc_network.name
    subnetwork = google_compute_subnetwork.vpc_subnetwork.name
    access_config {
      // Ephemeral public IP
    }
  }

  metadata = {
    enable-oslogin = "TRUE"
  }

  metadata_startup_script = templatefile("startup.sh", {
    root_password        = var.mysql_params.root_password
    custom_user          = var.mysql_params.custom_user
    custom_user_password = var.mysql_params.custom_user_password
    ddl                  = var.mysql_params.ddl
  })

  service_account {
    email  = data.google_compute_default_service_account.gce_account.email
    scopes = ["cloud-platform"]
  }
}

# Setup network firewalls
resource "google_compute_firewall" "allow-mysql" {
  depends_on  = [google_compute_subnetwork.vpc_subnetwork]
  project     = var.common_params.project
  name        = "allow-mysql"
  network     = google_compute_network.vpc_network.name
  description = "Allow traffic from private connectivity endpoint of Datastream"

  allow {
    protocol = "tcp"
    ports    = ["3306"]
  }
  source_ranges = [var.vpc_params.private_conn_cidr_range]
  target_tags   = ["databases"]
}

resource "google_compute_firewall" "allow-dataflow" {
  depends_on  = [google_compute_subnetwork.vpc_subnetwork]
  project     = var.common_params.project
  name        = "allow-dataflow"
  network     = google_compute_network.vpc_network.name
  description = "Allow traffic between Dataflow VMs"

  allow {
    protocol = "tcp"
    ports    = ["12345-12346"]
  }
  source_tags = ["dataflow"]
  target_tags = ["dataflow"]
}

resource "google_spanner_instance" "spanner_instance" {
  depends_on       = [google_project_service.enabled_apis]
  config           = var.spanner_params.config
  display_name     = var.spanner_params.display_name
  name             = var.spanner_params.name
  processing_units = var.spanner_params.processing_units
}

resource "google_spanner_database" "spanner_database" {
  depends_on          = [google_spanner_instance.spanner_instance]
  instance            = google_spanner_instance.spanner_instance.name
  name                = var.spanner_params.database_name
  ddl                 = var.spanner_params.ddl
  deletion_protection = false
}