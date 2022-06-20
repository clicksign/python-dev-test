#!/bin/bash
sudo rm -rf etl.log
echo "alias l='ls -lah --group-directories-first'" >>/home/ec2-user/.bashrc
echo '================================================================' >/home/ec2-user/etl.log
echo 'RAFT - 16 junho 2022' >>/home/ec2-user/etl.log
echo 'bootstrap da máquina EC2' >>/home/ec2-user/etl.log
echo '================================================================' >>/home/ec2-user/etl.log
pip3 install pandas >>/home/ec2-user/etl.log
echo '================================================================' >>/home/ec2-user/etl.log
pip3 show pandas >>/home/ec2-user/etl.log
echo '================================================================' >>/home/ec2-user/etl.log
pip3 install db-sqlite3 >>/home/ec2-user/etl.log
echo '================================================================' >>/home/ec2-user/etl.log
pip3 show db-sqlite3 >>/home/ec2-user/etl.log
echo '================================================================' >>/home/ec2-user/etl.log
pip3 install boto3 >>/home/ec2-user/etl.log
echo '================================================================' >>/home/ec2-user/etl.log
pip3 show boto3 >>/home/ec2-user/etl.log
echo '================================================================' >>/home/ec2-user/etl.log
aws s3 cp s3://nome-do-bucket/crontab.txt /home/ec2-user/ >>/home/ec2-user/etl.log
aws s3 cp s3://nome-do-bucket/etl.py /home/ec2-user/ >>/home/ec2-user/etl.log
aws s3 cp s3://nome-do-bucket/params.py /home/ec2-user/ >>/home/ec2-user/etl.log
aws s3 cp s3://nome-do-bucket/finaliza.sh /home/ec2-user/ >>/home/ec2-user/etl.log
echo 'ls -la /home/ec2-user/' >>/home/ec2-user/etl.log
ls -la /home/ec2-user/ >>/home/ec2-user/etl.log
echo '================================================================' >>/home/ec2-user/etl.log
echo "* * * * * sleep  0;python3 /home/ec2-user/etl.py >> /home/ec2-user/etl.log" >>crontab.txt
echo "* * * * * sleep 10;python3 /home/ec2-user/etl.py >> /home/ec2-user/etl.log" >>crontab.txt
echo "* * * * * sleep 20;python3 /home/ec2-user/etl.py >> /home/ec2-user/etl.log" >>crontab.txt
echo "* * * * * sleep 30;python3 /home/ec2-user/etl.py >> /home/ec2-user/etl.log" >>crontab.txt
echo "* * * * * sleep 40;python3 /home/ec2-user/etl.py >> /home/ec2-user/etl.log" >>crontab.txt
echo "* * * * * sleep 50;python3 /home/ec2-user/etl.py >> /home/ec2-user/etl.log" >>crontab.txt
crontab crontab.txt
echo '$ crontab -l' >>/home/ec2-user/etl.log
crontab -l >>/home/ec2-user/etl.log
echo '================================================================' >>/home/ec2-user/etl.log
echo 'chmod +x /home/ec2-user/etl.py' >>/home/ec2-user/etl.log
chmod +x /home/ec2-user/etl.py
echo 'chmod +x /home/ec2-user/finaliza.py' >>/home/ec2-user/etl.log
chmod +x /home/ec2-user/finaliza.sh
ls -la >>/home/ec2-user/etl.log
echo '================================================================' >>/home/ec2-user/etl.log
echo $(date) >>/home/ec2-user/etl.log
echo 'fim do setup inicial' >>/home/ec2-user/etl.log
echo '================================================================' >>/home/ec2-user/etl.log
