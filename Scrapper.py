from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
lista = []
lista2 = []
index = 1
count = 0

# html = '<html><body><div><div><main><div><div><div><div class="GameStats_game_times__KHrRY shadow_shadow"><ul><li><h5></h5></ul></li></div></div></div></div></main></div></div></body></html>'
# soup = BeautifulSoup.BeautifulSoup(html)

# page = requests.get(f"https://howlongtobeat.com/game/1",headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"})
# soup = BeautifulSoup(page.text, "html.parser")
# print(soup.body.div(attrs={"id":"__next"})[0].div(attrs={"class":"Layout_countainer___dzs2"}))



while count <= 30:
    file = open(f"scrapped_data{count}.csv", "w")
    for Num in range(0,5000): 
        print(f"Pagina https://howlongtobeat.com/game/{Num}")
        page = requests.get(f"https://howlongtobeat.com/game/{Num}",headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"})
        soup = BeautifulSoup(page.text, "html.parser")
        lista.append(soup.find_all("li", class_=re.compile("^GameStats_short__tSJ6I time_")))

    df = pd.DataFrame(lista)
    df.columns = ["Main", "Main + Sides", "Completionist", "All Styles"]

    df.to_csv(file)