terraform {
  backend "gcs" {
    bucket = "btibert-ba882-fall25-terraform"
    prefix = "terraform/state"
  }
}