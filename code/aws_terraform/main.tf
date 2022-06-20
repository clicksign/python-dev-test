terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = "us-east-2"
}

resource "aws_instance" "bureau-ec2" {
  # source  = "terraform-aws-modules/ec2-instance/aws"
  # version = "~> 3.0"


  ami                    = "ami-02d1e544b84bf7502"
  instance_type          = "t2.micro"
  key_name               = "bureau"
  monitoring             = true
  vpc_security_group_ids = ["sg-abacaxi1234567"]
  subnet_id              = "subnet-5428353c"
  iam_instance_profile   = "EC2-S3-teste"

  tags = {
    Name        = "us-bureau"
    Terraform   = "true"
    Environment = "dev"
  }



  connection {
    type        = "ssh"
    user        = "ec2-user"
    private_key = file("/home/god/chave.pem")
    host        = self.public_ip

  }

  provisioner "remote-exec" {

    inline = [
      "sudo aws s3 cp s3://nome-do-bucket/start.sh /etc/init.d/",
      "sudo chmod +x /etc/init.d/start.sh",
      "sudo rm -rf /home/ec2-user/etl.log",
      "sudo /etc/init.d/start.sh"
    ]
  }

}
