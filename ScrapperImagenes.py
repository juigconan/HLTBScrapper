from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import os
imageList = []
numList = []
imageFile = "scrapped_data_image.csv"
start = 69068
end = 151000
# Descomentar para rehacer el archivo
# if os.path.exists("./"+imageFile): os.remove("./"+imageFile)

for gameIndex in range(start, end): 
    print(f"Pagina https://howlongtobeat.com/game/{gameIndex}")
    page = requests.get(f"https://howlongtobeat.com/game/{gameIndex}",headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"})
    soup = BeautifulSoup(page.text, "html.parser")
    imageList.append(soup.find_all("div", class_=re.compile("^GameSideBar_game_image__")))
    numList.append(gameIndex)

    try:
            dfImage = pd.DataFrame(imageList)
            dfIndex = pd.DataFrame(numList)
            dfIndexImage = pd.merge(dfIndex, dfImage, left_index=True, right_index=True)
            dfIndexImage.columns = ["hltbIndex","imageSrc"]
            dfIndexImage.to_csv(imageFile, mode='a', header=not os.path.exists(imageFile))
            imageList = []
            numList = []
    except:
        imageList = []
        numList = []
