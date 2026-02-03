Create ssh tunnel into ec2.

Open up cmd prompt and enter the following command:
ssh -i "ssh.pem" -L 5433:database-test1.co5okk6ow6qt.us-east-1.rds.amazonaws.com:5432 ec2-user@23.21.11.130

Need to have ssh.pem key in the same folder as where the cmd prompt is located. 
This uses our public EC2 instance as a "bridge" to reach our private RDS database.


If you get this error:
Permissions for 'ssh.pem' are too open.
It is required that your private key files are NOT accessible by others.
This private key will be ignored.
Load key "ssh.pem": bad permissions


Use these commands:
icacls ssh.pem /reset
icacls ssh.pem /grant:r "%USERNAME%":R
icacls ssh.pem /inheritance:r