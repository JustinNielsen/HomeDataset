import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
import time
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor

def get_listing_details(url):
    driver = uc.Chrome() 
    driver.get(url)
    time.sleep(1)

    # Click the first two "Show more" buttons with re-locating after each click
    try:
        for i in range(2):  # Limit to the first two buttons
            show_more_buttons = driver.find_elements(By.XPATH, "//button[.//span[text()='Show more']]")
            if i < len(show_more_buttons):
                driver.execute_script("arguments[0].click();", show_more_buttons[i])  # Click each button using JavaScript
                time.sleep(1)  # Short delay to allow content to load after each click
            else:
                print(f"Less than {i+1} 'Show more' buttons found on {url}")
                break
    except Exception as e:
        print(f"Error clicking 'Show more' buttons on {url}: {e}")
    
    # Get page source and parse with BeautifulSoup
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()  # Close the browser after scraping

    # Extract year built
    year_built = None
    year_built_text = soup.find("span", string=re.compile(r"Built in \d{4}"))
    if year_built_text:
        year_built_match = re.search(r"\d{4}", year_built_text.text)
        if year_built_match:
            year_built = year_built_match.group()

    # Extract date on market and calculate days_on_market
    days_on_market = None
    date_on_market_text = soup.find("span", string=re.compile(r"Date on market:"))
    if date_on_market_text:
        date_on_market_str = re.search(r"\d{1,2}/\d{1,2}/\d{4}", date_on_market_text.text).group()
        date_on_market = datetime.strptime(date_on_market_str, "%m/%d/%Y")
        days_on_market = (datetime.now() - date_on_market).days

    # Extract school ratings for K-6, 7-9, and 10-12
    k_6_rating = -1
    grade_7_9_rating = -1
    grade_10_12_rating = -1

    school_elements = soup.find_all("li")
    for school in school_elements:
        grade_span = school.find(string=re.compile(r"Grades:"))
        if grade_span:
            grade_text = grade_span.find_next("span").text
            rating_span = school.find("span", class_=re.compile(r".*"))  # Less specific rating selector
            rating_text = rating_span.text if rating_span else "-1"

            try:
                rating = int(rating_text)
            except ValueError:
                rating = -1 
            
            # Assign rating based on grade level
            if "K-" in grade_text or "1-" in grade_text:
                k_6_rating = rating
            elif "6-" in grade_text or "7-" in grade_text:
                grade_7_9_rating = rating
            elif "-12" in grade_text:
                grade_10_12_rating = rating

    return year_built, days_on_market, k_6_rating, grade_7_9_rating, grade_10_12_rating



def process_urls_from_csv(input_csv, output_csv):
    # Read the CSV file
    df = pd.read_csv(input_csv)
    total_urls = len(df)

    # Prepare lists to store results
    year_built_list = []
    days_on_market_list = []
    k_6_rating_list = []
    grade_7_9_rating_list = []
    grade_10_12_rating_list = []

    # Process each URL sequentially with a counter
    for index, url in enumerate(df['detail_url'], start=1):
        print(f"Processing URL {index}/{total_urls}: {url}")
        try:
            year_built, days_on_market, k_6_rating, grade_7_9_rating, grade_10_12_rating = get_listing_details(url)
            year_built_list.append(year_built)
            days_on_market_list.append(days_on_market)
            k_6_rating_list.append(k_6_rating)
            grade_7_9_rating_list.append(grade_7_9_rating)
            grade_10_12_rating_list.append(grade_10_12_rating)
        except Exception as e:
            print(f"Error processing URL: {url} - {e}")
            year_built_list.append(None)
            days_on_market_list.append(None)
            k_6_rating_list.append(-1)
            grade_7_9_rating_list.append(-1)
            grade_10_12_rating_list.append(-1)

    # Add the results to the DataFrame
    df['year_built'] = year_built_list
    df['days_on_market'] = days_on_market_list
    df['k_6_rating'] = k_6_rating_list
    df['7_9_rating'] = grade_7_9_rating_list
    df['10_12_rating'] = grade_10_12_rating_list

    # Save the updated DataFrame to a new CSV file
    df.to_csv(output_csv, index=False)
    print(f"Results saved to {output_csv}")

# Example usage
input_csv = 'homes_5.csv'
output_csv = 'homes_5_v2.csv'
process_urls_from_csv(input_csv, output_csv)
