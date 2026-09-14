To put resources on a GitHub repository, follow these steps:

1- Install Git on the machine
2- Open PowerShell
3- cd "C:\Hes-so\Codes\Swissgrid data to influxdb"
4- Initialize Git in this directory: git init -b main
5- Add files you wand to upload on GitHub:
	git add .\main.py .\Requirements.txt

6- Create the 1st commit:
	git commit -m "initial version"

7- Connect the local folder to GitHub repository:
	git remote add origin http://.....git

8- Upload files
	git push -u origin main