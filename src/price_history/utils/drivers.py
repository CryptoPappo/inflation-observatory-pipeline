from selenium import webdriver
from selenium.webdriver.firefox.options import Options

def set_up():
    options = Options()
    options.add_argument("--headless")
    firefox_binary_path = r"/snap/firefox/current/usr/lib/firefox/firefox"
    options.binary_location = firefox_binary_path
    driver = webdriver.Firefox(options=options)
    
    return driver
