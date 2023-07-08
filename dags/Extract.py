import requests as rs
import json
import pandas as pd

url = "https://covid-19.dataflowkit.com/v1"
#url_all = "https://covid-19.dataflowkit.com/v1/world"


response = rs.get(url).json()
data_rs = json.dumps(response, indent=2)

## Extract Data

## Hitung Total Banyaknya Neagara 
def hitung_banyak_dict_dalam_json() -> int:
    data = json.loads(data_rs)
    count = 0
    if isinstance(data, list): #jika data mengandung data type = list maka akan lanjut 
        for elemen in data:
            if isinstance(elemen, dict): #jika elemen mengandung type data dict maka count bertambah 1
                count += 1
    return count

## masukiun hasil get data lalu di masukan ke dalam LIST
def buat_list() -> list:
    all_data = []
    for lop in range(hitung_banyak_dict_dalam_json()-1):  #dikurang 1 karna data terahkir hanyak tanggal atau last update
        data = response[lop]
        all_data.append(data)
    return all_data



