from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
import numpy as np
import pandas as pd
from urllib.parse import urlparse
import requests 

def validateURL(url):
    parsed_url = urlparse(url)
    return bool(parsed_url.scheme) and bool(parsed_url.netloc)

class Status:
    def __init__ (self, downloadedSuccessful, message):
        self.downloadedSuccessful = downloadedSuccessful
        self.message = message

    def downloadSucceded(self):
        return self.downloadedSuccessful

def downloadPDF(url, file_path):
    if pd.isnull(url) or not validateURL(url):
        return Status(False, f"The given url is not an URL {url}") 

    try:
        response = requests.get(url, stream=True)  # stream=True allows handling large files better
        if response.status_code != 200:
            return Status(False, f"Failed to download PDF. Status code: {response.status_code}") #(False, f"Failed to download PDF. Status code: {response.status_code}")
        content_type = response.headers.get("Content-Type")
        if content_type != "application/pdf":
            return Status(False, f"No PDF found at the specified URL. Content-Type: {content_type}") #(False, f"No PDF found at the specified URL. Content-Type: {content_type}")
        with open(file_path, 'wb') as pdf_file:
            pdf_file.write(response.content)
        return Status(True, f"PDF downloaded successfully: {file_path}")

    except Exception as e:
        return Status(False, f"Failed to download PDF. Error: {e}")


def processPDFDownloadsInParallel(df):
    # Split the DataFrame into chunks
    num_chunks = 2
    df_chunks = np.array_split(df, num_chunks)
    
    # Process each chunk into Pools for better update preformance on the df
    with Pool() as pool:
        results = pool.map(processPDFDownloadChunk, df_chunks)
    
    # Combine the processed chunks back into a single DataFrame
    final_df = pd.concat(results, ignore_index=True)
    return final_df


def processPDFDownload(index_row_tuple):
    index, row = index_row_tuple 
    result = row.copy()  # Create a copy of the row to avoid changing the original DataFrame
    if not row["PDF_URL_downloaded"]:
        file_path_pdf = f"pdf/{row['BRnum']}.pdf"
        
        # Attempt primary download URL
        status = downloadPDF(row["Pdf_URL"], file_path_pdf)
        if status.downloadSucceded(): #checks to see if the download was successfull
            result["PDF_URL_downloaded"] = True
        
        elif not row["Report Html Address"]:
            # Try the secondary download if the first failed and a backup URL exists
            statusSecond = downloadPDF(row["Report Html Address"], file_path_pdf)
            if statusSecond.downloadSucceded():
                result["PDF_URL_downloaded"] = True
                result["download_message"] = "Report Html Address was used"
            else:
                # Append error message from second attempt
                result["download_message"] = f"Error {status.message} | error from Report Html Address: {statusSecond.message}"
        
        else:
            
            result["download_message"] = f"Error {status.message}"
        print(status.message)
    return index, result  # Return index to know where to update the DataFrame


def processPDFDownloadChunk(df_chunks):
    # Use ThreadPoolExecutor to process each row in parallel 
    # becuase its heavly based on I/O operations and ThreadPoolExecutor is good for that
    max_threads = 2
    # Use max_threads to limit concurrent threads in the ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        results = list(executor.map(processPDFDownload, df_chunks.iterrows()))
    
    # Apply changes back to the DataFrame as the last step 
    # to not slow down the download threads becuase updating 
    # the df is more CPU bound and not I/O bound
    for index, updated_row in results:
        df_chunks.loc[index] = updated_row  

    return df_chunks


def processPDFDownloads(df):
    for index, row in df.iterrows():
        #    print(f"Row {index}:")
        if not row["PDF_URL_downloaded"]:
            file_path_pdf = f"pdf/{row["BRnum"]}.pdf"
            status = downloadPDF(row["Pdf_URL"], file_path_pdf)
            if status[0]:
                df.loc[index, "PDF_URL_downloaded"] = True
            else:
                print(status[1])
                df.loc[index, "download_message"] = f"Error {status[1]}"
                if pd.notnull(row["Report Html Address"]):
                    statusSecond = downloadPDF(row["Report Html Address"], file_path_pdf)
                    if statusSecond[0]:
                        df.loc[index, "PDF_URL_downloaded"] = True
                        df.loc[index, "download_message"] = "Report Html Address was used" 
                        #print(statusSecond[1])
                    else:
                        df.loc[index, "download_message"] += f" | error from Report Html Address: {statusSecond[1]}"
      #  if index > 20:
      #      break