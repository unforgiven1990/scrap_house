import pandas as pd
import numpy as np
import selenium
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc
from LB import *
import os.path
from selenium import webdriver
import time
import os
import re
#import pinyin
from pypinyin import pinyin, lazy_pinyin, Style
import shutil
from PIL import Image

from pathlib import Path
from pandas.api.types import is_string_dtype
from bs4 import BeautifulSoup
from pandas.api.types import is_numeric_dtype
from deep_translator import (GoogleTranslator,
                             MicrosoftTranslator,
                             PonsTranslator,
                             LingueeTranslator,
                             MyMemoryTranslator,
                             YandexTranslator,
                             PapagoTranslator,
                             DeeplTranslator,
                             QcriTranslator,
                             single_detection,
                             batch_detection)


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
englishchar="abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ"


df_path= pd.DataFrame() # stores all path of all projects
def initiate(project):
    df_path.at[f"{project}","root"]=helper=f'{Path.cwd()}/{project}/'
    Path(helper).mkdir(parents=True, exist_ok=True)
    for n in range(5):
        pass
        #df_path.at[f"{project}",f"step{n}"]=saver=f'{Path.cwd()}//{project}//step{n}//'
        #Path(saver).mkdir(parents=True, exist_ok=True)


def path_step_result(step=0, type='.xlsx'):
    return f"{df_path.at['Douban',f'root']}/step{step}{type}"

def step1():
    """where to get the input list
    can be manual
    can be conducted
    """
    return

def replace_cn_name(name):
    for ch in [":", "?", ">", "<", "|", '"', "*", "/", "！", "·", "—", " "]:
        name=name.replace(ch,"")
    return name

#def details_scrap(df_urls, output_path):
def step2(driver="", slice=[]):

    a_result=[]
    driver = uc.Chrome(version_main=108) if not driver else driver
    driver.maximize_window()  # maximize window size
    df_step1=pd.read_excel(path_step_result(1), sheet_name="all").set_index("url", drop=True)

    for counter, url in enumerate(df_step1.index[::1]):
        #get to website, backup plan if time out exception
        for n in range(10):
            try:
                driver.get(url)
                time.sleep(5)
                break
            except:
                time.sleep(60)

        dict_result={"url":url,
                     "EN Name":"",
                     "Youtube":"",
                     "MangoTV":"",
                     "iQiyi":"",
                     "Tencent":"",
                     "Youku":"",
                     "Bilibili":"",
                     "Other":"",
                     "Weibo":"",
                     "Image Google":"",
                     "Image Bing":"",
                     "Image Baidu":"",
                     "Raw Folder":"",
                     }

        #data that I scrap manually
        manual_input_dict = {
            "CN Name": [driver.find_element, "css selector", "h1 span", {}],
            "Alt Name": [get_text_after_element, driver, "//span[text()='又名:']", {}],
            "Genre": [driver.find_element, By.XPATH, "//span[@property='v:genre']", {}],
            "Year": [driver.find_element,"css selector", "h1 .year", {"modes":[0]}],
            "First EP": [driver.find_element, By.XPATH, "//span[@property='v:initialReleaseDate']",{"modes": [8], "trimafternumb": 10}],
            "Season": [get_text_after_element,driver, "//span[text()='季数:']", {"modes":[0]}],
            "EP": [get_text_after_element,driver, "//span[text()='集数:']", {"modes":[0]}],
            "IMDB": [get_text_after_element,driver, "//span[text()='IMDb:']", {"modes":[6,9], "formatstring": "https://www.imdb.com/title/{string}/"}],
            "Actors": [driver.find_element, "css selector", "#info .actor",    {"modes": [3], "replacedict": {"主演:": "", " / ": ", "}}],
            "Votings": [driver.find_element, By.XPATH, "//span[@property='v:votes']", {"modes": [0]}],
            "Rating": [driver.find_element,"css selector", ".ll.rating_num", {"modes":[2]}],
            "Comments": [driver.find_element,"css selector", "#comments-section h2 a", {"modes":[0]}],
            "Summary": [driver.find_element, By.XPATH, "//span[@property='v:summary']", {}],
        }

        # data that I scrap in batch like from lianjia
        auto_input_dict={}

        # clean up by each string. column wise str cleanup is more efficient
        for key, (func, arg1, arg2, cleanuparg) in manual_input_dict.items():
            try:
                helper = func(arg1, arg2)
                helper= helper.text if not isinstance(helper, str) else helper

                # word by word cleanup, might be inefficient if we do columnwise cleanup
                cleanuparg["string"]=helper
                dict_result[key]=cleanup(**cleanuparg)
            except Exception as e:
                dict_result[key] = ""


        cn_conv={
            "一":1,
            "二":2,
            "三":3,
            "四":4,
            "五":5,
            "六":6,
            "七":7,
            "八":8,
            "九":9,
            "十":10,
            "十一":11,
            "十二":12,
            "十三":13,
            "十四":14,
            "十五":15,
            "十六":16,
            "十七":17,
            "十八":18,
            "十九":19,
            "二十":20,
        }
        #correct season sometimes not added
        if "第" in dict_result["CN Name"] and "季" in dict_result["CN Name"]:
            try:
                last_part=dict_result["CN Name"].split(" ")[-1]
                last_part =last_part.replace("第","").replace("季","")
                arabic=cn_conv[last_part]
            except:
                arabic=None
            dict_result["Season"] = arabic



        #add manual click on link
        a_clickable=driver.find_elements("css selector", ".playBtn")
        for a in a_clickable:
            try:
                a.click()
                time.sleep(1)
                broadcaster=a.text
                popup_a=driver.find_element("css selector", "#tv-play-source a")

                url=popup_a.get_attribute("href")

            except:
                continue
            if "腾讯" in broadcaster:
                id="Tencent"
            elif "爱奇艺" in broadcaster:
                id="iQiyi"
            elif "芒果" in broadcaster:
                id="MangoTV"
            elif "优酷" in broadcaster:
                id="Youku"
            elif "哔哩哔哩" in broadcaster:
                id="Bilibili"
            else:
                id="Other"

            driver.get(url)
            url = driver.current_url
            dict_result[id] = url
        a_result+=[dict_result]
        print(f"Just Scraped Douban Step 2 {counter} {url}")


    #create df and pass to next function
    df_result=pd.DataFrame(a_result).set_index("url", drop=True)

    # column wise cleanup
    df_result["EN Name"]=df_result["Alt Name"].apply(lambda x: "".join([y for y in x if y in englishchar]).strip())
    df_result["Image Bing"]=df_result["CN Name"].apply(lambda x: f"https://cn.bing.com/images/search?q={x}&form=HDRSC2&first=1&tsc=ImageHoverTitle")
    df_result["Image Baidu"]=df_result["CN Name"].apply(lambda x: f"https://image.baidu.com/search/index?tn=baiduimage&ps=1&ct=201326592&lm=-1&cl=2&nc=1&ie=utf-8&dyTabStr=MCwzLDIsMSw2LDQsNSw4LDcsOQ%3D%3D&word={x}")
    df_result["Image Google"]=df_result["CN Name"].apply(lambda x: f"https://www.google.com.hk/search?newwindow=1&sxsrf=ALiCzsbjyNdqR4mK4KpFBh7mX4W-zbf1yg:1670375198081&q={x}&tbm=isch&sa=X&ved=2ahUKEwjNzMrCqOb7AhVPCYgKHaaxAysQ0pQJegQIEhAB&biw=1280&bih=647&dpr=1.5")
    df_result["Raw Folder"]=df_result["CN Name"].apply(lambda x: fr'{Path.cwd()}/Douban/Show/raw/{x}')
    df_result["Douban"]=df_result.index

    col_order = ["EN Name", "Summary", "Youtube", "MangoTV", "Tencent", "iQiyi", "Youku","Other", "IMDB", "Genre", "EP", "First EP", "Rating"]
    df_result = df_result[ col_order + [x for x in df_result.columns if x not in col_order]]
    df_result.to_excel(path_step_result(2), index=True)



def step3():
    """
    get link from all names
    :return:
    """
    driver = uc.Chrome(version_main=108)
    driver.maximize_window()  # maximize window size

    df_resultstep2 = pd.read_excel(path_step_result(2)).set_index("url", drop=True)
    url_template="https://www.google.de/search?q="


    for url, cn_name in zip(df_resultstep2.index,df_resultstep2["CN Name"]):
        # get to website, backup plan if time out exception
        for n in range(10):
            try:
                driver.get(f"{url_template}{cn_name}")
                time.sleep(5)
                break
            except:
                time.sleep(60)

        dict_links = {
                       "CN Wikipedia": "zh.wikipedia.org",
                       "EN Wikipedia": "en.wikipedia.org",
                       "Baidu": "baike.baidu.com",
                       "IMDB": "imdb.com",
                       "Tencent": "v.qq.com",
                       "MangoTV": "mgtv.com",
                       "iQiyi": "iqiyi.com/",
                       "Youku": "youku.com",
                       "Bilibili": "bilibili.com",
                       "Weibo": "weibo.com",
                       "Youtube Scrap": "youtube.com",
                       "Ole": "olevod",
                       }

        h3_list=driver.find_elements(By.XPATH,"//h3/..")
        for h3 in h3_list:
            h3_url=h3.get_attribute("href")
            if h3_url is None:
                continue
            print(h3_url)
            for key,val in dict_links.items():
                if val in h3_url:
                    df_resultstep2.at[url,key]=h3_url


        print(f"Just Googled SPER Step 3  {cn_name}")
    df_resultstep2.to_excel(path_step_result(3), index=True)




def step4():
    """
    translate data to english language
    """
    df_result=pd.read_excel(path_step_result(3)).set_index("url",drop=True)
    a_columns=["Genre","Actors","Summary"]
    model = GoogleTranslator(source='zh-CN', target='en')
    for col in a_columns:
        for key,val in df_result[col].items():
            try:
                if col=="Actors":
                    newcol="EN Actors"
                else:
                    newcol=col
                df_result.at[key,newcol]=helper= model.translate(val)
                print("translated col ",helper)
            except:
                pass

            try:# cut summary length to 80 words, otherwise it is too long
                if (col == "Summary"):
                    summary_length=60
                    word_count=len(val.split(" "))
                    if (word_count>summary_length): # too long, needs to be cut
                        reduced_summary = ""
                        for sentence in val.split("."):
                            reduced_summary += sentence
                            if len(reduced_summary.split(" ")) > summary_length:
                                break
            except:
                pass


    #cut down summary length
    df_result.to_excel(path_step_result(4), index=True)



def step5():
    """image processing create folder, rename image files"""
    df_result = pd.read_excel(path_step_result(4)).set_index("url", drop=True)
    df_step5=pd.DataFrame()
    for en_name, cn_name in zip(df_result["EN Name"],df_result["CN Name"]):
        cn_name=replace_cn_name(cn_name)
        path_raw=df_path.at[f"Douban", "root"]+f"Show/raw/{cn_name}"
        #path_upload=df_path.at[f"Douban", "root"]+f"Show/to upload/{cn_name}"
        #path_shopify=df_path.at[f"Douban", "root"]+f"Show/shopify/{cn_name}"
        Path(path_raw).mkdir(parents=True, exist_ok=True)
        df_step5.at[cn_name,"link"]=path_raw
    #rename all files to project name
    pass

    path_raw = df_path.at[f"Douban", "root"] + f"Show/raw/"
    path_upload = df_path.at[f"Douban", "root"] + f"Show/upload/"
    Path(path_upload).mkdir(parents=True, exist_ok=True)


    #copy image to other folder
    for (root, dirs, files) in os.walk(path_raw, topdown=True):
        print(root, dirs, files)
        for counter,file in enumerate(files):
            #just copying image

            #skip file already generated



            try:
                source=root+"/"+file
                cn_show=root.split("/")[-1]
                cn_name = replace_cn_name(cn_show)
                #pinyin_show=pinyin.get(cn_name, format="strip", delimiter="_")# old pinyin method with wrong pinyin
                pinyin_show="-".join(lazy_pinyin(cn_name))

                destination=path_upload+pinyin_show+f"_{counter}.webp"

                if os.path.isfile(destination):
                    print("destination skipped ", destination)
                    continue
                #shutil.copy(source, destination)

                picture = Image.open(source)
                width, height = picture.size

                # check if image is horizontal

                target_width=1000

                #picture = picture.resize((target_width, round(target_width/width*height,0)), Image.ANTIALIAS)
                picture = picture.resize((target_width, int(target_width/width*height)))
                picture.save(destination,
                             "webp",
                             optimize=True,
                             quality=70)
                print("destination created ",destination)
            except Exception as e :
                pass
                #print(e) #exception due to finding a folder

            #compressing image

    #then manually add all pictures to the folder to upload to shopify





def step6():
    """convert excel data to formated english article """


    df_result_master = pd.read_excel(path_step_result(4)).set_index("url", drop=True)
    colums_to_article = []
    for col in df_result_master.columns:
        if "A_" in col:
            colums_to_article+=[col]


    for arti in colums_to_article:
        df_result=df_result_master[df_result_master[arti].notna()]
        df_result=df_result.sort_values(arti)

        soup = BeautifulSoup("<html><body></body></html>", "html.parser")
        dict_base=[ "EN Name","Summary" ]
        dict_video=["Youtube","MangoTV","iQiyi","Tencent","Youku","Bilibili","Ole","Other"]
        dict_article=["IMDB","Douban", "Weibo","CN Wikipedia","Baidu"]
        dict_display = ["Summary", "Alt Name", "Rating", "Genre", "First EP", "Season", "EP", "Actors"]
        df_result=df_result[dict_display+[x for x in df_result.columns if x not in dict_display]]
        df_result = df_result[dict_article + [x for x in df_result.columns if x not in dict_article]]
        df_result=df_result[dict_video+[x for x in df_result.columns if x not in dict_video]]
        df_result=df_result[dict_base+[x for x in df_result.columns if x not in dict_base]]
        for counter, (url, row) in enumerate(df_result.iterrows()):
            #run over first time to create label
            for key,val in row.items():
                if pd.isnull(val) and key not in ["Summary","EN Name", "Youtube"]:
                    continue

                try:
                    if key in ["Genre", "Alt Name"]:
                        val=val.title()
                except:
                    pass


                if key in ["EN Name"]:
                    if pd.isnull(val):
                        val = row['CN Name']
                    tag = soup.new_tag("h2")
                    tag.string = f"{counter+1}. {val} ({row['CN Name']})"
                    soup.html.body.append(tag)
                elif key in ["Summary"]:
                    tag = soup.new_tag("div")
                    tag.string = f"{key}: {str(val).replace('.0','')}" if key !="Summary" else f"{str(val).replace('.0','')}"
                    soup.html.body.append(tag)
                    ul = soup.new_tag("ul")
                    soup.html.body.append(ul)

                    #after summary hardcode add doubank link
                    """
                    li = soup.new_tag("li")
                    a = soup.new_tag("a")
                    a.string = f"Douban Article"
                    a.attrs["href"] = url
                    a.attrs["target"] = "_blank"
                    b = soup.new_tag("b")
                    b.append(a)
                    li.append(b)
                    ul = soup.select("ul")[-1]
                    ul.append(li)"""
                elif key in ["Actors"]:
                    #add chinese name to english actor list
                    li = soup.new_tag("li")
                    li.string = f"{key}: {row['EN Actors']} ({str(val).replace('.0','')})"
                    ul=soup.select("ul")[-1]
                    ul.append(li)
                elif key in dict_display:
                    if key=="Rating" and val==0:
                        continue# do not write rating 0
                    li = soup.new_tag("li")
                    li.string = f"Douban {key}: {str(val).replace('.0','')}" if key=="Rating" else f"{key}: {str(val).replace('.0','')}"
                    ul=soup.select("ul")[-1]
                    ul.append(li)
                elif key in dict_video:
                    li = soup.new_tag("li")
                    a = soup.new_tag("a")
                    a.string = f"{key} Playlist" if key=="Youtube" else f"{key} Video"
                    a.attrs["href"]=val
                    a.attrs["target"]="_blank"
                    b = soup.new_tag("b")
                    b.append(a)
                    li.append(b)
                    ul = soup.select("ul")[-1]
                    ul.append(li)
                elif key in dict_article:
                    li = soup.new_tag("li")
                    a = soup.new_tag("a")
                    a.string = f"{key} Article"
                    a.attrs["href"]=val
                    a.attrs["target"]="_blank"
                    b = soup.new_tag("b")
                    b.append(a)
                    li.append(b)
                    ul = soup.select("ul")[-1]
                    ul.append(li)


                if key=="Summary":
                    soup.html.body.append(soup.new_tag("br"))

                if key in ["Youtube"]:
                    try:
                        print(val)
                        if val:
                            youtube_id = val.split("watch?v=")[1]
                        elif row["Youtube Scrap"]:
                            youtube_id = row["Youtube Scrap"].split("watch?v=")[1]
                        else:
                            continue
                        if "&" in youtube_id:
                            youtube_id = youtube_id.split("&")[0]
                        else:
                            continue
                        #youtube_id = val.split("&")[0]
                    except Exception as e:
                        print("youtube error ",e)
                        continue

                    iframe = soup.new_tag("iframe")
                    iframe.attrs["width"] = "100%"
                    iframe.attrs["height"] = "315"
                    iframe.attrs["src"] = f"https://www.youtube.com/embed/{youtube_id}"
                    iframe.attrs["title"] = f"{row['EN Name']}"
                    iframe.attrs["frameborder"] = 0
                    iframe.attrs["allow"] = "accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                    iframe.attrs["allowfullscreen"] = ""
                    soup.html.body.append(iframe)

            #add images from own device
            """
            root_path_en=df_path.at[f"Douban", "root"]+f"Show/{row['CN Name']}"
    
            for (root, dirs, files) in os.walk(root_path_en, topdown=True):
                for file in files:
                    tag = soup.new_tag("img")
                    tag.attrs["src"] = root+"/"+file
                    soup.html.body.append(tag)
            """

            #upload image from shopify

            try:
                clean_en_name=replace_cn_name(row['CN Name'])
                #cn_name_pinyin=pinyin.get(clean_en_name,delimiter="_", format="strip")
                cn_name_pinyin = "-".join(lazy_pinyin(clean_en_name))
            except Exception as e:
                print(e)
                continue

            print(clean_en_name)
            root_path_en = df_path.at[f"Douban", "root"] + f"Show/upload"

            #shopify_url="https://cdn.shopifycdn.net/s/files/1/0687/0145/4658/files/sheng_sheng_bu_xi_6.webp"
            shopify_url="https://cdn.shopifycdn.net/s/files/1/0687/0145/4658/files/"
            for (root, dirs, files) in os.walk(root_path_en, topdown=True):

                for file in files:
                    if cn_name_pinyin in file:
                        clean_en_name = replace_cn_name(file)
                        tag = soup.new_tag("img")
                        tag.attrs["src"] = shopify_url  + clean_en_name+"?v=1670384452"
                        soup.html.body.append(tag)
                    else:
                        pass


            soup.html.body.append(soup.new_tag("br"))
            soup.html.body.append(soup.new_tag("br"))
            soup.html.body.append(soup.new_tag("br"))


        str_content = str(soup)
        with open(f"{df_path.at['Douban',f'root']}/step6_{arti}.html", "w", encoding='utf-8') as text_file:
            text_file.write(str_content)





def proxy():
    prox = "157.254.193.139:80"
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--proxy-server=%s' % prox)

    driver = uc.Chrome(version_main=108, chrome_options=chrome_options)
    #driver = uc.Chrome(version_main=108)
    driver.maximize_window()  # maximize window size
    driver.get("https://www.google.de/")
    #driver.get("https://whatismyipaddress.com/")
    print("here")
    time.sleep(5000)


if __name__ == '__main__':
    initiate(project="Douban")
    #step1()
    functions = [step1,step2,step3,step4,step5,step6]
    """
    1. Get URL
    2. Get Details
    3. get google links
    4. Translate text to english
    5. add image folder and create upload folder
    6. convert into text article
       
    """
    do=[5,6]
    for number  in do:
        func=functions[number-1]
        func()




