from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import os
timeList = []
nameList = []
numList = []
failList = []
timeFile = "scrapped_data_time.csv"
nameFile = "scrapped_data_name.csv"
failFile = "scrapped_data_fail.csv"
start = 0
end = 151000
# Descomentar para rehacer los archivos
#if os.path.exists("./"+timeFile): os.remove("./"+timeFile)
#if os.path.exists("./"+failFile): os.remove("./"+failFile)
#if os.path.exists("./"+failFile): os.remove("./"+failFile)

for gameIndex in range(start, end): 
    print(f"Pagina https://howlongtobeat.com/game/{gameIndex}")
    page = requests.get(f"https://howlongtobeat.com/game/{gameIndex}",headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"})
    soup = BeautifulSoup(page.text, "html.parser")
    timeList.append(soup.find_all("li", class_=re.compile("^GameStats_short__tSJ6I time_")))
    nameList.append(soup.find_all("div", class_="GameHeader_profile_header__q_PID shadow_text"))
    numList.append(gameIndex)
    try:
            dfTime = pd.DataFrame(timeList)
            dfName = pd.DataFrame(nameList)
            dfIndex = pd.DataFrame(numList)
            dfIndexTime = pd.merge(dfIndex, dfTime, left_index=True, right_index=True)
            dfIndexName = pd.merge(dfIndex, dfName, left_index=True, right_index=True)
            dfIndexTime.columns = ["hltbIndex","Main", "Main + Sides", "Completionist", "All Styles"]
            dfIndexTime.to_csv(timeFile, mode='a', header=not os.path.exists(timeFile))
            dfIndexName.to_csv(nameFile, mode='a', header=not os.path.exists(nameFile))
            timeList = []
            nameList = []
            numList = []
    except:
        failList.append(gameIndex)
        timeList = []
        nameList = []
        numList = []
        dfFail = pd.DataFrame(failList)
        dfFail.to_csv(failFile, mode='a', header=False)
        failList = []
