import pandas as pd
import numpy as np
import selenium
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc


def get_text_after_element(driver,xpath_element,sibiling_type="span"):
    """
    1. find his next element
    2. find his parents
    3. get all text from his parents
    4. get the substring between two elements
    """

    element=driver.find_element(By.XPATH,xpath_element)
    parent = driver.find_element(By.XPATH, f"{xpath_element}/parent::*")
    try:#if this element is also the last element, then we just disregard the sibiling
        silbling=driver.find_element(By.XPATH,f"{xpath_element}/following-sibling::{sibiling_type}")
        result = parent.text.split(element.text)[1]  # disregard everything before the element
        result = result.split(silbling.text)[0]  # disregard everything after the elements sibiling
    except Exception as e:
        result = parent.text.split(element.text)[1]  # disregard everything before the element
    return result


def number_extractor(s,type=float):
    result=""
    try:
        for ch in s:
            if ch.isdigit() or ch==".":
                result += ch
        return type(result)
    except:
        return type(0)



def cleanup(string, modes=[1], trimbeforechar="", trimafterchar="",trimbeforenumb=-1, trimafternumb=-1, replacedict={}, formatstring=""):
    """
    there are 3 ways to clean up:
    0. extract numbers only and conver it to int
    1. leave it as it is
    2. extract numbers only and conver it to float
    3. replace certain words with nothing
    4. trim everything before certain words
    5. trim everything after certain words
    6. trim all empty strings and caps
    7. trim everything before the nth char
    8. trim everything after the nth char
    9. add string as a substring to another string txt = "For only {price} dollars!"
    """

    for number in modes:
        if 0 ==number:
            string=number_extractor(string,type=int)
        if 1 ==number:
            string=string
        if 2 ==number:
            string=number_extractor(string,type=float)
        if 3 ==number:
            for key, val in replacedict.items():
                string = string.replace(key, val)
        if 4 ==number:
            string=string.split(trimbeforechar)[1]
        if 5 ==number:
            string=string.split(trimafterchar)[0]
        if 6 ==number:
            string=string.strip()
        if 7 ==number:
            string=string[trimbeforenumb:]
        if 8 ==number:
            string=string[:trimafternumb]
        if 9 ==number:
            string=formatstring.format(string=string)
    return string