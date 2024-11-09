To run this program you need a ftp server and the excel file 

first step.

update the config file in the repository,
set the host to adrress of the ftp and a path to a credential file.
Path to excel file and desired chunks to split the download into and 
how many threads each chunks will use, threads and chunks are both 
used for relevant tasks.

[ftp]
host = localhost
userEnvFilePath = C:\credFolder\cred.env


[settings]
filePath = C:\Users\spac-36\Downloads\GRI_2017_2020.xlsx
chunks = 8
chucksPerThreat = 10

Second step 

create a .env file with the following format so the solution can connect to the ftp
MY_APP_USERNAME=username
MY_APP_PASSWORD=password

The solution has a requirements file
