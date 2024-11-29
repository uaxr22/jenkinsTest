import pytest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



#driver = webdriver.Chrome(executable_path="C:\Users\Oluwafemi\Documents\sample data_python/chromedriver.exe")


import pytest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

@pytest.fixture


def driver():
    # Specify the path to the chromedriver executable
     chrome_service = Service(r"C:\Users\Oluwafemi\Documents\sample data_python\chromedriver.exe")
     #driver = webdriver.Chrome("C:\\Users\\Oluwafemi\\Downloads\\chromedriver-win32.exe")
     #chrome_service = Service(r"C:\Users\Oluwafemi\Downloads\chromedriver-win32.exe")
  # Initialize Chrome options
     chrome_options = Options()
     chrome_options.add_argument("--no-sandbox")
     chrome_options.add_argument("--disable-dev-shm-usage")
     chrome_options.add_argument("--headless")  # Optional: Run the browser in headless mode (without UI)
    
    # Initialize the Chrome WebDriver with the Service and Options
     driver = webdriver.Chrome(service=chrome_service, options=chrome_options)


     yield driver
     driver.quit()

def test_login(driver):
    # Open the login page
    driver.get("https://example.com/login")

    # Wait until the username field is present
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "username")))

    # Enter username
    driver.find_element(By.ID, "username").send_keys("testuser")

    # Enter password
    driver.find_element(By.ID, "password").send_keys("password123")

    # Click the login button
    driver.find_element(By.ID, "login-button").click()

    # Wait until the title contains "Dashboard" after logging in
    WebDriverWait(driver, 10).until(EC.title_contains("Dashboard"))

    # Assert that the title of the page includes "Dashboard"
    assert "Dashboard" in driver.title
