from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import csv
import os
timeList = []
nameList = []
numList = []
failList = []
timeFile = "scrapped_data_time.csv"
nameFile = "scrapped_data_name.csv"
failFile = "scrapped_data_fail.csv"
start = 29832
#if os.path.exists("./"+file1): os.ºremove("./"+file1)
#if os.path.exists("./"+file2): os.remove("./"+file2)

for gameIndex in range(start, 151000): 
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
