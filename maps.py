from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import csv

driver = webdriver.Chrome()
driver.get("https://www.google.com/maps")

time.sleep(5)

businesses = [
    ["ABC Restaurant", "+1 234 567 890", "New York", "abc.com", "4.5"],
    ["XYZ Cafe", "+1 987 654 321", "New York", "xyzcafe.com", "4.3"]
]

with open("sample_output.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["Name", "Phone", "Address", "Website", "Rating"])
    writer.writerows(businesses)

driver.quit()
