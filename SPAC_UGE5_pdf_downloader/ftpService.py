from ftplib import FTP
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

class FTPService:
    def __init__ (self):
        self.ftp = None

    def getListOfExsistingFiles(self):
        return self.ftp.nlst()

    def connect(self):
        try:
            load_dotenv(r"C:\credFolder\cred.env")  # Load .env file to geet cred
            username = os.getenv("MY_APP_USERNAME")
            password = os.getenv("MY_APP_PASSWORD")
            # Connect to the server and log in
            self.ftp = FTP('localhost')
            self.ftp.login(user=username, passwd=password) 
            return (True, "successfully connected")
        except Exception as e:
            self.ftp.quit()
            return (False, f"Connection failed {e}")
        
    def uploadFiles(self, filename):
        try:
            with open(filename, 'rb') as file:
                self.ftp.storbinary(f'STOR {filename}', file)
        except Exception as e:
            return (False, f"Connection failed {e}")        

    def closeConnection(self):
        self.ftp.quit()

    def uploadFilesInParallel(self, filenames):
        results = []
        with ThreadPoolExecutor() as executor:
            # Submit each file upload as a separate task
            futures = {executor.submit(self.uploadFile, filename): filename for filename in filenames}
            for future in as_completed(futures):
                # Collect the result of each upload
                result = future.result()
                results.append(result)
        return results