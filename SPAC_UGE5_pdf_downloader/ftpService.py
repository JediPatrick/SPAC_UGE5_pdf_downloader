from ftplib import FTP
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

class FTPService: ## make the connect the first function to be called
    def __init__ (self):
        self.ftp = None

    def getDictOfExsistingFiles(self):
        listOfExsistingFiles = self.ftp.nlst()
        hashTableOfExsistingFiles = {item: True for item in listOfExsistingFiles}
        return hashTableOfExsistingFiles

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
        
    def uploadFile(self, filename):
        try:
            with open(f"pdf/{filename}", 'rb') as file:
                self.ftp.storbinary(f'STOR {filename}', file)
            return (True, "successfully uploaded")
        except Exception as e:
            return (False, f"Connection failed {e}")        

    def closeConnection(self):
        self.ftp.quit()

    def uploadFilesInParallel(self, df):
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
           # if index > 20:
           #     break