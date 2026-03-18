from selenium import webdriver
from selenium.webdriver.firefox.options import Options

def set_up():
    options = Options()
    options.add_argument("--headless")
    firefox_binary_path = r"/snap/firefox/current/usr/lib/firefox/firefox"
    options.binary_location = firefox_binary_path
    driver = webdriver.Firefox(options=options)
    return driver

if __name__ == "__main__":
    driver = set_up()
    url = "https://www.cotodigital.com.ar/sitios/cdigi/productos/categorias"
    driver.get(url)
    pagination = driver.find_elements(By.CLASS_NAME, "col-xl-4.col-lg-4.col-md-6.col-sm-6.col-xs-12.mb-4.producto-card.ng-star-inserted")
    item = pagination[0]
    item_ = item.find_element(By.CLASS_NAME, "d-flex.justify-content-end")
    item_.get_attribute("href")
