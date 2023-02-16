import pandas as pd
import numpy as np
import selenium
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc
from LB import *
import os.path
import time
import re
from pathlib import Path
from pandas.api.types import is_string_dtype
from bs4 import BeautifulSoup
from pandas.api.types import is_numeric_dtype


"""
Scraping procedure
library, used by all scrapers
step 1 get all urls to scrap
step 2 get all details + clean up
step 3 do statistics on it
step 5 result saver

solve partial step 1, step 2
"""

"""path includes
relative path
project name, what to scrap, house or stock, or car, or imdb
download data, list of all urls
progress data, raw data, or statistic data, or what
"""
englishchar = "abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ"

df_path = pd.DataFrame()  # stores all path of all projects


def initiate(project):
    df_path.at[f"{project}", "root"] = helper = f'{Path.cwd()}//{project}//'
    Path(helper).mkdir(parents=True, exist_ok=True)
    for n in range(5):
        pass


def path_step_result(step=0, type='.xlsx'):
    return f"{df_path.at['Rental', f'root']}/step{step}{type}"


def step1():
    """
    manually get url of all subway lines

    """
    return

def rental_step2():
    """
    go to each subway line to get the station
    :return:
    """
    df_result=pd.DataFrame()
    driver = uc.Chrome(version_main=106)
    driver.maximize_window()  # maximize window size
    df_step1 = pd.read_excel(path_step_result(1)).set_index("url", drop=True)

    for counter, url in enumerate(df_step1.index[::1]):
        # get to website, backup plan if time out exception
        for n in range(10):
            try:
                driver.get(url)
                break
            except:
                time.sleep(60)

        line=driver.find_elements(By.XPATH, "//li[contains(@class, 'strong')]/a")[2]
        stations_list=driver.find_elements(By.XPATH, "(//ul[@data-target='station'])[2]//a")
        for station in stations_list:
            if station.text=="不限":
                continue
            station_url=station.get_attribute("href")
            df_result.at[station_url,"station"]=station.text
            df_result.at[station_url,"line"]=line.text
            print(station.text,", ",station_url,", ",line.text)
    df_result.to_excel(path_step_result(2), index=True)

def rental_step3():
    driver = uc.Chrome(version_main=106)
    driver.maximize_window()  # maximize window size
    df_step2 = pd.read_excel(path_step_result(2)).set_index("url", drop=True)
    df_result = pd.DataFrame()
    for url,station,line in zip(df_step2.index,df_step2["station"],df_step2["line"]):
        # get to website, backup plan if time out exception
        for n in range(10):
            try:
                driver.get(url)
                break
            except:
                time.sleep(60)

        try:
            content_list=driver.find_element("css selector", ".content__list")
            list_url=content_list.find_elements("css selector", ".content__list--item--aside")
            list_price=content_list.find_elements("css selector", "span.content__list--item-price")
            list_size=content_list.find_elements("css selector", "p.content__list--item--des")
            for url,price,size in zip(list_url,list_price,list_size):
                url=url.get_attribute("href")
                df_result.at[url,"price"]=number_extractor(price.text,type=int)
                size_helper=(size.text).split("/")[1]
                size_helper=size_helper.replace("㎡","")
                df_result.at[url,"size"]=number_extractor(size_helper,type=float)
                df_result.at[url,"station"]=station
                df_result.at[url,"line"]=line
        except:
            pass


    df_result.to_excel(path_step_result(3), index=True)


def rental_step4():
    #do statistic on it
    df_result=pd.read_excel(path_step_result(3)).set_index("url",drop=True)

    #add price per square meter
    df_result["price/size"]=df_result["price"]/df_result["size"]


    # group by column
    list_df = {}
    for col in df_result.columns:
        try:
            if is_string_dtype(df_result[col]):
                list_df[col] = df_result.groupby(col).mean().sort_values(by=['price/size'])
        except:
            pass

    #save excel as backup
    with pd.ExcelWriter(path_step_result(4)) as writer:
        df_result.to_excel(writer, sheet_name="df_result", index=True)
        for key, val in list_df.items():
            val.to_excel(writer, sheet_name=key, index=True)
    # save excel as modifiable
    with pd.ExcelWriter(path_step_result(4,type="_edtiable.xlsx")) as writer:
        df_result.to_excel(writer, sheet_name="df_result", index=True)
        for key, val in list_df.items():
            val.to_excel(writer, sheet_name=key, index=True)

# def details_scrap(df_urls, output_path):
def step2(driver="", slice=[]):
    a_result = []
    driver = uc.Chrome(version_main=106) if not driver else driver
    driver.maximize_window()  # maximize window size
    df_step1 = pd.read_excel(path_step_result(1)).set_index("url", drop=True)

    for counter, url in enumerate(df_step1.index[::1]):
        # get to website, backup plan if time out exception
        for n in range(10):
            try:
                driver.get(url)
                break
            except:
                time.sleep(60)

        dict_result = {"url": url,
                       "EN Name": "",
                       "Youtube": "",
                       "MangoTV": "",
                       "iQiyi": "",
                       "Tencent": "",
                       "Other": "",
                       }

        # data that I scrap manually
        manual_input_dict = {
            "CN Name": [driver.find_element, "css selector", "h1 span", {"modes": [5], "trimafterchar": " "}],
            "Alt Name": [get_text_after_element, driver, "//span[text()='又名:']", {}],
            "Genre": [driver.find_element, By.XPATH, "//span[@property='v:genre']", {}],
            "Year": [driver.find_element, "css selector", "h1 .year", {"modes": [0]}],
            "First EP": [driver.find_element, By.XPATH, "//span[@property='v:initialReleaseDate']", {"modes": [8], "trimafternumb": 10}],
            "Season": [get_text_after_element, driver, "//span[text()='季数:']", {"modes": [0]}],
            "EP": [get_text_after_element, driver, "//span[text()='集数:']", {"modes": [0]}],
            "IMDB": [get_text_after_element, driver, "//span[text()='IMDb:']", {"modes": [6, 9], "formatstring": "https://www.imdb.com/title/{string}/"}],
            "Actors": [driver.find_element, "css selector", "#info .actor", {"modes": [3], "replacedict": {"主演:": "", " / ": ", "}}],
            "Votings": [driver.find_element, By.XPATH, "//span[@property='v:votes']", {"modes": [0]}],
            "Rating": [driver.find_element, "css selector", ".ll.rating_num", {"modes": [2]}],
            "Comments": [driver.find_element, "css selector", "#comments-section h2 a", {"modes": [0]}],
            "Summary": [driver.find_element, By.XPATH, "//span[@property='v:summary']", {}],
        }

        # data that I scrap in batch like from lianjia
        auto_input_dict = {}

        # clean up by each string. column wise str cleanup is more efficient
        for key, (func, arg1, arg2, cleanuparg) in manual_input_dict.items():
            try:
                helper = func(arg1, arg2)
                helper = helper.text if not isinstance(helper, str) else helper

                # word by word cleanup, might be inefficient if we do columnwise cleanup
                cleanuparg["string"] = helper
                dict_result[key] = cleanup(**cleanuparg)
            except Exception as e:
                dict_result[key] = ""

        a_result += [dict_result]
        print(f"Just Scraped Douban Step 2 {counter} {url}")

    # create df and pass to next function
    df_result = pd.DataFrame(a_result).set_index("url", drop=True)

    # column wise cleanup
    df_result["EN Name"] = df_result["Alt Name"].apply(lambda x: "".join([y for y in x if y in englishchar]).strip())
    df_result.to_excel(path_step_result(2), index=True)


def step3():
    pass


def step4():
    # do statistic on it
    df_result = pd.read_excel(path_step_result(3)).set_index("url", drop=True)

    # group by column
    list_df = {}
    for col in df_result.columns:
        try:
            if is_string_dtype(df_result[col]):
                list_df[col] = df_result.groupby(col).mean()
        except:
            pass

    # save excel as backup
    with pd.ExcelWriter(path_step_result(4)) as writer:
        df_result.to_excel(writer, sheet_name="df_result", index=True)
        for key, val in list_df.items():
            val.to_excel(writer, sheet_name=key, index=True)
    # save excel as modifiable
    with pd.ExcelWriter(path_step_result(4, type="_edtiable.xlsx")) as writer:
        df_result.to_excel(writer, sheet_name="df_result", index=True)
        for key, val in list_df.items():
            val.to_excel(writer, sheet_name=key, index=True)


def step5():
    """convert excel data to formated english text"""
    try:
        df_result = pd.read_excel(path_step_result(99)).set_index("url", drop=True)
    except:
        print("cannot load manual excel")
        df_result = pd.read_excel(path_step_result(4)).set_index("url", drop=True)

    col_order = ["EN Name", "Summary", "Youtube", "MangoTV", "Tencent", "iQiyi", "Other", "IMDB", "Genre", "EP", "First EP", "Rating"]
    df_result = df_result[["EN Name", "Summary", "Youtube", "MangoTV", "Tencent", "iQiyi", "Other", "IMDB", "Genre", "EP", "First EP", "Rating"] + [x for x in df_result.columns if x not in col_order]]
    soup = BeautifulSoup("<html><body></body></html>", "html.parser")
    dict_title = ["EN Name", ]
    dict_display = ["Genre", "First EP", "Summary", "Season", "EP", "Rating"]
    dict_link = ["Youtube", "MangoTV", "iQiyi", "Tencent", "Other", "IMDB"]
    for url, row in df_result.iterrows():
        # run over first time to create label
        for key, val in row.items():
            if pd.isnull(val) and (key != "EN Name"):
                continue
            if key in dict_title:
                if pd.isnull(val):
                    val = row['CN Name']
                tag = soup.new_tag("h2")
                tag.string = f"{val} ({row['CN Name']})"
                soup.html.body.append(tag)
            elif key in dict_display:
                tag = soup.new_tag("div")
                tag.string = f"{key}: {str(val).replace('.0', '')}"
                soup.html.body.append(tag)
            elif key in dict_link:
                tag = soup.new_tag("div")
                a = soup.new_tag("a")
                a.string = f"{key} Link"
                a.attrs["href"] = val
                tag.append(a)
                soup.html.body.append(tag)
            if key == "Summary":
                soup.html.body.append(soup.new_tag("br"))

        soup.html.body.append(soup.new_tag("br"))
        soup.html.body.append(soup.new_tag("br"))
        soup.html.body.append(soup.new_tag("br"))
    str_content = str(soup)
    with open(path_step_result(5, ".html"), "w", encoding='utf-8') as text_file:
        text_file.write(str_content)


def step6():
    """image processing create folder, rename image files"""
    df_result = pd.read_excel(path_step_result(99)).set_index("url", drop=True)
    for en_name, cn_name in zip(df_result["EN Name"], df_result["CN Name"]):
        if pd.isnull(en_name):
            Path(df_path.at[f"Douban", "root"] + f"Show/no en/{cn_name}").mkdir(parents=True, exist_ok=True)

        else:
            Path(df_path.at[f"Douban", "root"] + f"Show/en/{cn_name}").mkdir(parents=True, exist_ok=True)

    # rename all files to project name



def join_excel():
    excel1="join/kauf.xlsx"
    excel2="join/mieten.xlsx"
    excel1key = "最近站"
    excel2key = "station"

    df1=pd.read_excel(excel1)
    df2=pd.read_excel(excel2)
    print (df1)
    df=pd.merge(df1,df2,left_on=excel1key,right_on=excel2key, how="outer")
    df.to_excel("join/join.xlsx")





if __name__ == '__main__':
    initiate(project="Rental")
    # step1()
    functions = [step1, rental_step2,rental_step3,rental_step4]
    do = []
    join_excel()
    for number in do:
        func = functions[number - 1]
        func()




