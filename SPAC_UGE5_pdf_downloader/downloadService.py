from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
import numpy as np
import pandas as pd
from urllib.parse import urlparse
import requests 

class Status:
    def __init__ (self, downloadedSuccessful, message):
        self.downloadedSuccessful = downloadedSuccessful
        self.message = message

    def downloadSucceded(self):
        return self.downloadedSuccessful


def validateURL(url):
    parsed_url = urlparse(url)
    return bool(parsed_url.scheme) and bool(parsed_url.netloc)


def downloadPDF(url, file_path):
    if pd.isnull(url) or not validateURL(url):
        return Status(False, f"The given url is not an URL {url}") 

    try:
        response = requests.get(url, stream=True)  # stream=True allows handling large files better
        if response.status_code != 200:
            return Status(False, f"Failed to download PDF. Status code: {response.status_code}") 
        content_type = response.headers.get("Content-Type")
        if content_type != "application/pdf":
            return Status(False, f"No PDF found at the specified URL. Content-Type: {content_type}")
        with open(file_path, 'wb') as pdf_file:
            pdf_file.write(response.content)
        return Status(True, f"PDF downloaded successfully: {file_path}")

    except Exception as e:
        return Status(False, f"Failed to download PDF. Error: {e}")


def processPDFDownloadsInParallel(df, numChunks, threadsPerChunks):
    # Split the DataFrame into chunks
    df_chunks = np.array_split(df, numChunks)
    
    # Process each chunk into Pools for better update preformance on the df
    arguments = [(chunk, threadsPerChunks) for chunk in df_chunks]    
    with Pool() as pool:
        results = pool.starmap(processPDFDownloadChunk, arguments)
    
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
                result["download_message"] = f"Error {status.message} | error from Report Html Address: {statusSecond.message}"
        else:
            result["download_message"] = f"Error {status.message}"
        print(f"BRnummer {row['BRnum']}: {status.message}")
    return index, result  # Return index to know where to update the DataFrame


def processPDFDownloadChunk(df_chunks, max_threads):
    # Use ThreadPoolExecutor to process each row in parallel 
    # becuase its heavly based on I/O operations and ThreadPoolExecutor is good for that
    # Use max_threads to limit concurrent threads in the ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        results = list(executor.map(processPDFDownload, df_chunks.iterrows()))
    
    # Apply changes back to the DataFrame as the last step 
    # to not slow down the download threads becuase updating 
    # the df is more CPU bound and not I/O bound
    for index, updated_row in results:
        df_chunks.loc[index] = updated_row  

    return df_chunks
