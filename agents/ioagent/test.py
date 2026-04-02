url = "https://crmantra3.my.salesforce.com/sfc/dist/version/renditionDownload?rendition=JPGZ&versionId=068f6000000zt1B&operationContext=DELIVERY&contentId=05Tf6000001fiFp&page=0&d=/a/f60000006QMP/RjJOJOb1KK_k9Gt6Hq0HWi4eVVKof8X9fb_cYn8W_C0&oid=00Df600000Fvzw1&dpt=null&viewId"
splitted_url=url.split("/")
final_url=(f"https://crmantra3.my.salesforce.com/sfc/dist/version/renditionDownload?rendition=JPGZ&versionId=068f6000000zt1B&operationContext=DELIVERY&contentId=05Tf6000001fiFp&page=0&d=/a/{splitted_url[-2]}/{splitted_url[-1]}&oid=00Df600000Fvzw1&dpt=null&viewId")
filename = "downloaded_file.pdf"
print(final_url)    