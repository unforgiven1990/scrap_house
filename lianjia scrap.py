import sys
print(sys.path)
import pandas as pd
import urllib.request
import socket
import urllib.error

import re
import time
import random
from bs4 import BeautifulSoup

import glob
import os
import glob
import datetime
import shutil
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
import time
import os.path
import numpy as np

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from pathlib import Path
from datetime import datetime


#variable locations when using other laptops
#general
download_path="C://Users//DKE//Downloads//"
project_path='C://Users\DKE//OneDrive//Py_Scrap House//'
storage_path='C://Users\DKE//OneDrive//Py_Scrap House//files//'
transformed_path='C://Users\DKE//OneDrive//Py_Scrap House//transformed//'
today_global=datetime.today().strftime('%Y-%m-%d')


#ensure the path is created
Path(storage_path).mkdir(parents=True, exist_ok=True)
Path(transformed_path).mkdir(parents=True, exist_ok=True)

def number_extractor(s):
    result=""
    for ch in s:
        if ch.isdigit() or ch==".":
            result += ch
    return float(result)



def lianjia_scraping(city="sh", offset=1,today=today_global):
    Path(f'C://Users\DKE//OneDrive//Py_Scrap House//{today}//').mkdir(parents=True, exist_ok=True)
    Path(f'C://Users\DKE//OneDrive//Py_Scrap House//{today}//{today}_{city}').mkdir(parents=True, exist_ok=True)
    save_counter = 500 # how often data should be stored to excel
    excel_colum = ["url", "progress"]
    driver = uc.Chrome(version_main=106)  # creating a webdriver object
    driver.maximize_window()  # maximize window size



    #Phase 0: get all district 区，名字
    district_template=f"https://{city}.lianjia.com/xiaoqu/"
    driver.get(district_template)
    district_names=[]
    list = driver.find_elements("css selector", "div[data-role] a")
    final_raw = fr'{today}/{today}_{city}/{today}_{city}_all_raw.xlsx'
    if not os.path.isfile(final_raw):
        for element in list:
            try:
                district = element.get_attribute('href')
                district=district.split("/")[-2]
                district_names += [district]
            except:
                pass
        print("districtnames",district_names)



    # Phase 1: get all area 地区，名字
    area_names = {}
    final_raw = fr'{today}/{today}_{city}/{today}_{city}_all_raw.xlsx'
    if not os.path.isfile(final_raw):
        for district in district_names:
            area_template = f"https://{city}.lianjia.com/xiaoqu/"+district
            driver.get(area_template)
            time.sleep(5)
            list = driver.find_elements("css selector", "div[data-role] div")[1]
            list = list.find_elements("css selector", "a")
            for element in list:
                try:
                    area = element.get_attribute('href')
                    area = area.split("/")[-2]
                    if area=="client":
                        continue
                    area_names[area]= district
                except:
                    pass
        print("areanames",area_names)



    #Phase 2: Get all URL overview. fast in UC scrap, slow in housing scrap
    # do by each city district area
    for area,district in area_names.items():
        url_template = f"https://{city}.lianjia.com/ershoufang/{area}/pg"
        output_excel = f"{today}/{today}_{city}/{today}_{city}_{area}.xlsx"

        #output_excel = f"{today}/{today}_{city}/{today}_{city}_{area}_listing.xlsx"
        if os.path.isfile (output_excel):
            print("House list file already exists: "+output_excel)
            continue
        elif area == "client":
            continue
        try:
            table = pd.read_excel(output_excel)
            table.set_index(excel_colum[0], inplace=True)
        except Exception as e:
            table=pd.DataFrame(columns=excel_colum)
            table.set_index(excel_colum[0], inplace=True)
            driver.get(url_template+"1"+f"l1l2bp0ep400")


            #method 1: straight forword steal with modified chrome
            counter = 1
            subscectionid=".noresultRecommend"
            while True:
                list = driver.find_elements("css selector", subscectionid)
                for item in list:
                    url = item.get_attribute('href')
                    print(counter, ": ", url)
                    table.at[url, "city"] = city
                    table.at[url, "district"] = district
                    table.at[url, "area"] = area

                counter = counter + 1
                if counter % save_counter == 0:
                    table.to_excel(output_excel, index=True)
                driver.get(url_template + str(counter)+"l1l2bp0ep400")
                time.sleep(3)

                if driver.find_elements(By.XPATH, "//a[text()='下一页']"): #check if next button exists
                    continue
                else:
                    break

            table.to_excel(output_excel, index=True)



    # Phase 3 aggregate all areas into one, add date, add city
    final_raw = fr'{today}/{today}_{city}/{today}_{city}_all_raw.xlsx'
    final_edit = fr'{today}/{today}_{city}/{today}_{city}_all_edit.xlsx'
    if os.path.isfile(final_edit):
        finalexcelsheet=pd.read_excel(final_edit)
        finalexcelsheet.set_index("url", inplace=True)
    elif os.path.isfile(final_raw):
        finalexcelsheet = pd.read_excel(final_raw)
        finalexcelsheet.set_index("url", inplace=True)
    else:
        finalexcelsheet = pd.DataFrame()
        filenames = glob.glob(project_path+f"{today}/{today}_{city}/*.xlsx")
        for file in filenames:
            #df = pd.concat(pd.read_excel(file, sheet_name=None), ignore_index=True, sort=False)
            df=pd.read_excel(file)
            finalexcelsheet = finalexcelsheet.append(df, ignore_index=True)

        finalexcelsheet.set_index("url", inplace=True)
        print("len BEFORE drop", len(finalexcelsheet))
        #finalexcelsheet.drop_duplicates(subset=None, keep="first", inplace=True)
        print("len AFTER drop",len(finalexcelsheet))
        finalexcelsheet.copy().to_excel(final_raw, index=True)
        finalexcelsheet.copy().to_excel(final_edit, index=True)
    print(finalexcelsheet)




    #Phase 4: Get all details
    for number, (url, progress) in enumerate(zip(finalexcelsheet.index[offset::1], finalexcelsheet["progress"][offset::1])):
        if not pd.isna(progress):
            print(city, number, " skip offseted url ", url)
            continue
        else:
            #go to website, mechanism to prevent timeout
            for n in range(10):
                try:
                    driver.get(url)
                    time.sleep(2)
                    print(url)
                    break
                except:
                    time.sleep(60)


            try:
                metro_ement=driver.find_element("css selector", "#around")
                driver.execute_script("arguments[0].scrollIntoView();", metro_ement)
            except :
                pass

            base_introcontent_span=driver.find_elements("css selector", "div.introContent .base span")
            base_introcontent_li=driver.find_elements("css selector", "div.introContent .base li")
            transaction_introcontent_span = driver.find_elements("css selector", "div.introContent .transaction span")
            dict_details = {}


            #info part 1 get basic data
            try:
                dict_details["总价"]=driver.find_element("css selector", "span.total").text
                dict_details["总价"]=float(dict_details["总价"])
            except:
                dict_details["总价"]=0

            for span, li in zip(base_introcontent_span,base_introcontent_li):
                dict_details[span.text]=li.text.replace(span.text,"")


            # info part 2 get transaction data
            for label, content in zip(transaction_introcontent_span[::2],transaction_introcontent_span[1::2]):
                dict_details[label.text]=content.text


            # info part 3 get real size data
            realize_elements = driver.find_elements("css selector", "#infoList div.col")
            realsize=0
            for element in realize_elements:
                try:
                    realsize+=number_extractor(element.text)
                except:
                    pass
            dict_details["实际面积"]=realsize


            #other info, favorite and build year
            try:
                dict_details["小区"] = driver.find_element("css selector", "div.communityName a").text
            except:
                dict_details["小区"] = ""
            try:
                dict_details["年份"]=driver.find_element("css selector", ".subInfo.noHidden").text
                dict_details["年份"]=int(dict_details["年份"].split("年")[0])
            except:
                dict_details["年份"]= 0

            try:
                dict_details["关注"] = int(driver.find_element("css selector", "span#favCount").text)
            except:
                dict_details["关注"] = 0

            try:
                dict_details["看过人数"] = int(driver.find_element("css selector", "span#cartCount").text)
            except:
                dict_details["看过人数"] = 0


            try:#days since 上牌
                dt_string = dict_details["挂牌时间"]
                listing_date = datetime.strptime(dt_string, "%Y-%m-%d")
                today_date = datetime.strptime(today, "%Y-%m-%d")
                difference=today_date-listing_date
                dict_details["已挂牌天"] = float(difference.days)
            except:
                dict_details["已挂牌天"] = 1

            try:#关注度/上牌日期
                dict_details["每日关注度"] = dict_details["关注"]/dict_details["已挂牌天"]
            except:
                dict_details["每日关注度"] = 0



            #地铁信息
            try:
                dict_details["周边站数"]=len(driver.find_elements("css selector", "#mapListContainer li"))
            except:
                dict_details["周边站数"]= 0

            try:
                dict_details["最近站"]=driver.find_element("css selector", ".itemTitle").text
            except:
                dict_details["最近站"]= ""
            try:
                dict_details["最近站距离"]=driver.find_element("css selector", ".itemdistance").text
                dict_details["最近站距离"] = float(dict_details["最近站距离"].replace("米",""))
            except:
                dict_details["最近站距离"]= 99999


            #clean up
            try:
                dict_details["建筑面积"]=float(dict_details["建筑面积"].replace("㎡",""))
                dict_details["得房率"] = dict_details["实际面积"] / dict_details["建筑面积"]
                dict_details["建筑面积单价"]=dict_details["总价"]/dict_details["建筑面积"]
                dict_details["实际面积单价"]=dict_details["总价"]/dict_details["实际面积"]
            except:
                pass

            try:
                dict_details["总楼层"] =number_extractor(dict_details["所在楼层"])
                dict_details["所在楼层"]=dict_details["所在楼层"][0:4]
            except:
                pass

            try:
                dict_details["套内面积"]=float(dict_details["套内面积"].replace("㎡",""))
            except:
                pass


            try:
                for key, val in dict_details.items():
                    finalexcelsheet.at[url, key] = val
                finalexcelsheet.at[url, "progress"] = 1
            except Exception as e:
                print(e)
                continue


            if number % 200 == 0:
                finalexcelsheet.to_excel(final_edit, index=True)
                print("saved")
    finalexcelsheet.to_excel(final_edit, index=True)

    return

    #Phas 3 cleanup
    print("Start phase 3")
    table["country"]=table["location"].str[-2:]
    table["location"]=table["location"].str[:-4]

    table.to_excel(output_final_excel, index=True)
    driver.close()





def test(date,city):
    url = fr'{date}/{date}_{city}/{date}_{city}_all_edit.xlsx'
    url_statistic = fr'{date}/{date}_{city}/{date}_{city}_all_stats.xlsx'
    data=pd.read_excel(url)

    # group by region
    list_df={}
    for col in ["最近站","所在楼层","户型结构","装修情况","配备电梯","房屋用途","房屋年限","产权所属","小区","年份","总楼层"]:
        df=data.groupby(col).mean()
        list_df[col]=df[["建筑面积单价","实际面积单价"]].sort_values(by=['建筑面积单价'])

    replace_dict=list_df["最近站"]["实际面积单价"].to_dict()
    print(replace_dict)
    data["地站均实际价格"]=data["最近站"]
    replaced_df=data.replace({"地站均实际价格":replace_dict})
    data["地站均实际价格"]=replaced_df["地站均实际价格"]
    data["地站均实际价格%"]=data["实际面积单价"]/data["地站均实际价格"]
    data=data.sort_values(by=['地站均实际价格%'])

    with pd.ExcelWriter(url_statistic) as writer:
        # use to_excel function and specify the sheet_name and index
        # to store the dataframe in specified sheet
        data.to_excel(writer, sheet_name="data", index=False)
        for key,val in list_df.items():
            val.to_excel(writer, sheet_name=key, index=True)

def proxy_check():
    try:
        urllib.urlopen(
            "www.google.de",
            proxies={'http': '145.239.85.58:9300'}
        )
    except IOError:
        print
        "Connection error! (Check proxy)"
    else:
        print
        "All was fine"


if __name__ == '__main__':
    #for city in ["sh","bj","sz","hk","hz","su"]:
    #test("2022-12-04","sh")
    #proxy_check()
    for city in ["sh"]:
        pass
        lianjia_scraping(city=city, offset=0,today=today_global)

