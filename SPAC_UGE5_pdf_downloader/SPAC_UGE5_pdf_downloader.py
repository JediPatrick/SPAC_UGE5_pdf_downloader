import pandas as pd
from downloadService import processPDFDownloadsInParallel
from ftpService import FTPService
import configparser


def importFile(file_path):
    # Load the Excel file into a DataFrame
    df = pd.read_excel(file_path)  # Specify the sheet if needed
    if "PDF_URL_downloaded" not in df.columns:
        df['PDF_URL_downloaded'] = False
    if "uploaded_To_FTP" not in df.columns:
        df['uploaded_To_FTP'] = False
    if "download_message" not in df.columns:
        df['download_message'] = ""
    return df


def main():
    config = configparser.ConfigParser()
    config.read("config.ini")
    
    ftpHost = config["ftp"]["host"]
    ftpUserEnvFilePath = config["ftp"]["userEnvFilePath"]
    excelFilePath = config["settings"]["filePath"]
    chunks = config.getint("settings", "chunks")
    chucksPerThreat = config.getint("settings", "chucksPerThreat")

    df = importFile(excelFilePath)
    df = processPDFDownloadsInParallel(df, chunks, chucksPerThreat)
    
    ftpService = FTPService(ftpUserEnvFilePath, ftpHost)
    ftpService.connect()
    ftpService.uploadFiles(df)
    ftpService.closeConnection()

    # update excel file, TODO: prederably only the updated coloumns should be updated 
    df.to_excel(excelFilePath, index=False) 

if __name__ == "__main__":
    main()







