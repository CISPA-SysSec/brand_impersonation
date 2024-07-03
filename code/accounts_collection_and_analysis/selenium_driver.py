import time

import platform

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


class PageDriver:
    def __init__(self, url):
        self.url = url
        self.driver = None

    def build_chrome_options(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.accept_untrusted_certs = True
        chrome_options.assume_untrusted_cert_issuer = True
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-impl-side-painting")
        chrome_options.add_argument("--disable-setuid-sandbox")
        chrome_options.add_argument("--disable-seccomp-filter-sandbox")
        chrome_options.add_argument("--disable-breakpad")
        chrome_options.add_argument("--disable-cast")
        chrome_options.add_argument("--disable-cast-streaming-hw-encoding")
        chrome_options.add_argument("--disable-cloud-import")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_argument("--disable-session-crashed-bubble")
        chrome_options.add_argument("--disable-ipv6")
        chrome_options.add_argument("--allow-http-screen-capture")
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--incognito")
        if platform.system().startswith("Linux"):
            chrome_options.add_argument("--headless")

        return chrome_options

    def get_driver(self):
        chrome_options = self.build_chrome_options()
        if platform.system().startswith("Linux"):
            self.driver = webdriver.Chrome(options=chrome_options)
        if platform.system().startswith("Darwin"):
            self.driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)

        self.driver.set_window_size(1200, 803)

        self.driver.get(self.url)
        return self.driver

    def scroll_to_bottom_of_page(self):
        try:
            lenOfPage = self.driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
            match = False
            while not match:
                lastCount = lenOfPage
                time.sleep(3)
                lenOfPage = self.driver.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return "
                    "lenOfPage;")
                if lastCount == lenOfPage:
                    match = True
        except:
            pass

    def try_waiting_xpath(self, x_path, wait_time=5):
        try:
            WebDriverWait(self.driver, wait_time).until(
                EC.visibility_of_element_located((By.XPATH, x_path)))
            elem = self.driver.find_element(By.XPATH, x_path)
            return elem

        except Exception as ex:
            print("Exception occurred in getting xpath {}, msg: {}".format(x_path, ex))

        return None

    def try_getting_href_and_text(self, link_href="//a[@href]"):
        _data = set()
        try:
            WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, link_href)))
            elems = self.driver.find_elements(By.XPATH, link_href)
            for elem in elems:
                found_href = elem.get_attribute("href")
                found_text = elem.text
                _data.add((found_href, found_text))
            return list(_data)
        except Exception as ex:
            print("Exception occurred while trying to read href, text".format(ex))

        return None

    def clean_up(self):
        try:
            self.driver.close()
            self.driver.quit()
        except Exception as ex:
            try:
                self.driver.close()
                self.driver.quit()
                print("Successfully closed browser after exception! {}".format(str(ex)))
            except Exception as ex_2:
                print("Unable to close browser as of some exceptions {}".format(str(ex_2)))

        finally:
            try:
                self.driver.close()
                self.driver.quit()
                print("Finally closed ... Successfully closed browser at final stage!")
            except Exception as ex:
                print("Except on finally closed .. : {}".format(str(ex)))