from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import os
lista = []
lista2 = []
lista3 = []
file1 = "scrapped_data_time.csv"
file2 = "scrapped_data_name.csv"
#if os.path.exists("./"+file1): os.remove("./"+file1)
#if os.path.exists("./"+file2): os.remove("./"+file2)

for Num in range(1,151000): 
    print(f"Pagina https://howlongtobeat.com/game/{Num}")
    page = requests.get(f"https://howlongtobeat.com/game/{Num}",headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"})
    soup = BeautifulSoup(page.text, "html.parser")
    lista.append(soup.find_all("li", class_=re.compile("^GameStats_short__tSJ6I time_")))
    lista2.append(soup.find_all("div", class_="GameHeader_profile_header__q_PID shadow_text"))
    lista3.append(Num)
    if(Num % 10 == 0 and Num != 0):
        df1 = pd.DataFrame(lista)
        df2 = pd.DataFrame(lista2)
        df3 = pd.DataFrame(lista3)
        df4 = pd.merge(df3, df1, left_index=True, right_index=True)
        df5 = pd.merge(df3, df2, left_index=True, right_index=True)
        df4.columns = ["hltbIndex","Main", "Main + Sides", "Completionist", "All Styles"]
        df4.to_csv(file1, mode='a', header=not os.path.exists(file1))
        df5.to_csv(file2, mode='a', header=not os.path.exists(file2))
        lista = []
        lista2 = []
        lista3 = []