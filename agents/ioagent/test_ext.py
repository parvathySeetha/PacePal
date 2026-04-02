import requests

url = "https://crmantra3.my.salesforce.com/sfc/dist/version/renditionDownload?rendition=JPGZ&versionId=068f6000000zt1B&operationContext=DELIVERY&contentId=05Tf6000001fiFp&page=0&d=/a/f60000006QMP/RjJOJOb1KK_k9Gt6Hq0HWi4eVVKof8X9fb_cYn8W_C0&oid=00Df600000Fvzw1&dpt=null&viewId"
filename = "downloaded_file.pdf"

try:
    response = requests.get(url, allow_redirects=True)
    
    print(f"Content Type received: {response.headers.get('Content-Type')}")
    
    # Only save if it is actually a PDF
    if "application/pdf" in response.headers.get("Content-Type", ""):
        with open(filename, 'wb') as pdf_file:
            pdf_file.write(response.content)
        print("Success: Real PDF downloaded.")
    else:
        print("Error: The URL returned a web page (likely login), not a PDF.")
        print("First 200 characters of response:", response.text[:200])

except Exception as e:
    print(f"An error occurred: {e}")