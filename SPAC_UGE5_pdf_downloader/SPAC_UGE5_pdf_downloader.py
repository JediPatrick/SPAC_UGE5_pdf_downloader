import pandas as pd
from downloadService import processPDFDownloads
from ftpService import FTPService

def importFile():
    # Load the Excel file into a DataFrame
    file_path = r"C:\Users\spac-36\Downloads\GRI_2017_2020.xlsx"  # Replace with your file path
    df = pd.read_excel(file_path)  # Specify the sheet if needed
    if "PDF_URL_downloaded" not in df.columns:
        df['PDF_URL_downloaded'] = False
    if "uploaded_To_FTP" not in df.columns:
        df['uploaded_To_FTP'] = False
    if "download_message" not in df.columns:
        df['download_message'] = ""
    return df

df = importFile()
processPDFDownloads(df)

    
ftpService = FTPService()
ftpService.connect()
ftpService.uploadFiles(df)
ftpService.closeConnection()
    

# Display the first few rows
print(df.head())




