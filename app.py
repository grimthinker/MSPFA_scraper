from bs4 import BeautifulSoup as bs
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import threading
from time import sleep

url_base = 'https://mspfa.com'
url_log = '/log/?s=26545'
max_threads = 6
result_data = []
filename = 'quested_webdata.csv'
pauseA = 4.4                        # Waiting time for a page to fully load
pauseB = 0.7                        # Time between thread's launches, to make some pause between making requests


def get_pages_urls_list():
    browser = webdriver.Chrome()
    browser.get(url_base+url_log)
    WebDriverWait(browser, pauseA).until(EC.presence_of_element_located((By.ID, "pages")))
    soup = bs(browser.page_source, 'html.parser')
    pgs_list_elem = soup.find("td", attrs={"id": "pages"}).find_all('a')
    pgs_list = tuple(pg_elem.attrs['href'] for pg_elem in pgs_list_elem)
    browser.close()
    return pgs_list


def separate_list(l: tuple, m: int):
    ceil = len(l)//m
    return (l[i:i+ceil] for i in range(0, len(l), ceil))


def get_data_from_pages(pages_pack: tuple):
    browser = webdriver.Chrome()
    for url_page in pages_pack:
        browser.get(url_base+url_page)
        sleep(pauseA)
        while True:
            try:
                browser.find_element(By.XPATH, "//img[@class='major']")
                break
            except NoSuchElementException:
                browser.refresh()
                sleep(pauseA)
        soup = bs(browser.page_source, 'html.parser')
        command = str(soup.find("div", attrs={"id": "command"}))
        content = str(soup.find("div", attrs={"id": "content"}))
        result_data.append((int(url_page[12:]), command, content))
    browser.close()


def write_in_file():
    df = pd.DataFrame(sorted(result_data, key=lambda x: x[0]), columns=['Number', 'Command', 'Content'])
    df.to_csv(filename, index=False)


def main():
    separated_gen = separate_list(get_pages_urls_list(), max_threads)
    for pack_for_thread in separated_gen:
        thread = threading.Thread(target=get_data_from_pages, args=(pack_for_thread, ))
        thread.start()
        sleep(pauseB)
    for t in threading.enumerate():
        if t != threading.main_thread():
            t.join()
    write_in_file()


if __name__ == '__main__':
    main()
