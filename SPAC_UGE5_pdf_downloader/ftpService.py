from ftplib import FTP
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

## the current FTP service does not handle threads well and is not thread safe prefereably another package should be used
# TODO: refactor this class to aiosftp
class FTPService: 
    def __init__ (self, envfile, host):
        self.ftp = None
        self.host = host 
        self.envfile = envfile

    def getDictOfExsistingFiles(self):
        listOfExsistingFiles = self.ftp.nlst()
        hashTableOfExsistingFiles = {item: True for item in listOfExsistingFiles}
        return hashTableOfExsistingFiles

    def connect(self):
        try:
            load_dotenv(self.envfile)  # Load .env file to geet cred
            username = os.getenv("MY_APP_USERNAME")
            password = os.getenv("MY_APP_PASSWORD")

            self.ftp = FTP(self.envfile)
            self.ftp.login(user=username, passwd=password) 
            return (True, "successfully connected")
        except Exception as e:
            self.ftp.quit()
            return (False, f"Connection failed {e}")
        
    def uploadFile(self, filename):
        try:
            with open(f"pdf/{filename}", 'rb') as file:
                self.ftp.storbinary(f'STOR {filename}', file)
            return (True, "successfully uploaded")
        except Exception as e:
            return (False, f"Connection failed {e}")        

    def closeConnection(self):
        self.ftp.quit()

    def uploadFilesInParallel(self, df): # parrallel is not ready to test 
        results = []
        with ThreadPoolExecutor() as executor:
            # Submit each file upload as a separate task
            futures = {executor.submit(self.uploadFile, filename): filename for filename in df}
            for future in as_completed(futures):
                # Collect the result of each upload
                result = future.result()
                results.append(result)
        return results

    def uploadFiles(self, df):
        exsistingFiles = self.getDictOfExsistingFiles()
        for index, row in df.iterrows():
            #    print(f"Row {index}:")
            if row["PDF_URL_downloaded"]:
                filename = f"{row["BRnum"]}.pdf"
                if filename in exsistingFiles:
                    print(f"file exist {filename}")
                else:
                    status = self.uploadFile(filename)                
                    if status[0]:
                        df.loc[index, "uploaded_To_FTP"] = True