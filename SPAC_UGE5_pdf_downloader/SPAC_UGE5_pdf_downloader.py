from urllib.parse import urlparse
import pandas as pd
import requests 
import concurrent.futures

from ftpService import FTPService

def validateURL(url):
    parsed_url = urlparse(url)
    return bool(parsed_url.scheme) and bool(parsed_url.netloc)

def downloadPDF(url, file_path):
    if pd.isnull(url) or not validateURL(url):
        return (False, f"The given url is not an URL {url}") 

    try:
        response = requests.get(url, stream=True)  # stream=True allows handling large files better
        if response.status_code != 200:
            return (False, f"Failed to download PDF. Status code: {response.status_code}")
        content_type = response.headers.get("Content-Type")
        if content_type != "application/pdf":
            return (False, f"No PDF found at the specified URL. Content-Type: {content_type}")
        with open(file_path, 'wb') as pdf_file:
            pdf_file.write(response.content)
        return (True, f"PDF downloaded successfully: {file_path}")

    except Exception as e:
        return (False, f"Failed to download PDF. Error: {e}")

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

for index, row in df.iterrows():
#    print(f"Row {index}:")
    if not row["PDF_URL_downloaded"]:
        file_path_pdf = f"pdf/{row["BRnum"]}.pdf"
        status = downloadPDF(row["Pdf_URL"], file_path_pdf)
        if status[0]:
            row["PDF_URL_downloaded"] = True
        else:
            print(status[1])
            row["download_message"] = f"Error {status[1]}"
            if pd.notnull(row["Report Html Address"]):
                statusSecond = downloadPDF(row["Report Html Address"], file_path_pdf)
                if statusSecond[0]:
                    row["PDF_URL_downloaded"] = True
                    row["download_message"] = "Report Html Address was used" 
                    #print(statusSecond[1])
                else:
                    row["download_message"] += f" | error from Report Html Address: {statusSecond[1]}"
    if index > 20:
        break
    
ftpService = FTPService()
ftpService.connect()
listOfFiles = ftpService.getListOfExsistingFiles()
ftpService.closeConnection()

#    print(row)

    

# Display the first few rows
print(df.head())




