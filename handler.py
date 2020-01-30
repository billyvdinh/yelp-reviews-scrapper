import os
import time
import json
import requests
from datetime import datetime
import psycopg2
from bs4 import BeautifulSoup


def handler(event, context):
    try:
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        print("Started at =", current_time)

        reviews = scrapper(is_local=False, business_name='studs-new-york')
        save2pg(is_local=False, reviews=reviews)

        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        print("Ended at =", current_time)
    except Exception as e:
        print(e)


def scrapper(is_local=True, business_name='first-bay-locksmith-santa-clara-3'):
    url = 'https://www.yelp.com/biz/' + business_name
    page = requests.get(url)
    content = BeautifulSoup(page.content, 'html.parser')
    region = content.find_all("div", {'role': 'navigation'})[0]
    page_count = int(region.find('span').text.split(' ')[-1])

    final_reviews = []
    for page_number in range(page_count):
        print('page number: {}'.format(page_number))

        time.sleep(5)
        url = 'https://www.yelp.com/biz/' + business_name + '?start={}'.format(page_number * 20)
        page = requests.get(url)

        content = BeautifulSoup(page.content, 'html.parser')
        tmp_reviews_1 = content.find_all("li", {'class': 'u-space-b3'})
        tmp_reviews_2 = content.find_all("li", {'class': 'u-padding-b3'})

        reviews = []
        for review in tmp_reviews_1:
            if review in tmp_reviews_2:
                reviews.append(review)

        for review in reviews:
            cleaned_review = {}
            review_content = review.find_all('div', recursive=False)
            left_review = review_content[0].find_all('div', recursive=False)[0]
            cleaned_review['avatar'] = left_review.find_all('img', srcset=True)[0]['src']
            passport_info = left_review.find_all('div', {'class': 'user-passport-info'})[0]
            cleaned_review['name'] = passport_info.find_all('span')[0].text
            cleaned_review['location'] = passport_info.find_all('span')[1].text
            items = left_review.find_all('div', {'class': 'u-space-r1'})

            cleaned_review['friends'] = ''
            cleaned_review['reviews'] = ''
            cleaned_review['photos'] = ''
            for item in items:
                text = item.find_all('span')[-1].text
                value = text.split(' ')[0]
                field = text.split(' ')[1]

                cleaned_review[field] = value

            right_review = review_content[0].find_all('div', recursive=False)[1]
            star_date = right_review.find_all('div', recursive=False)[0]
            cleaned_review['star'] = star_date.find_all('div', {'role': 'img'})[0]['aria-label'].split(' ')[0]
            cleaned_review['date'] = star_date.find_all('span')[-1].text

            cleaned_review['comment'] = right_review.find_all('span', lang=True)[0].text
            final_reviews.append(cleaned_review)

    return final_reviews


def save2pg(is_local, reviews):

    if not is_local:
        con = psycopg2.connect(
            host=os.environ['REDSHIFT_HOST'],
            port=os.environ['REDSHIFT_PORT'],
            database=os.environ['REDSHIFT_DBNAME'],
            user=os.environ['REDSHIFT_USERNAME'],
            password=os.environ['REDSHIFT_PASSWORD']
        )
    else:
        with open('config.json') as json_file:
            config = json.load(json_file)
        con = psycopg2.connect(
            host=config['REDSHIFT_HOST'],
            port=config['REDSHIFT_PORT'],
            database=config['REDSHIFT_DBNAME'],
            user=config['REDSHIFT_USERNAME'],
            password=config['REDSHIFT_PASSWORD']
        )

    cur = con.cursor()
    cur.execute('''DROP TABLE IF EXISTS yelp_reviews;''')
    con.commit()

    cur = con.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS yelp_reviews(
        avatar        VARCHAR(256) NOT NULL,
        name          VARCHAR(256) NOT NULL,
        location      VARCHAR(256) NOT NULL,
        friends       VARCHAR(5),
        reviews       VARCHAR(5),
        photos        VARCHAR(5),
        star          VARCHAR(5),
        date          VARCHAR(20),
        comment       VARCHAR(2000) NOT NULL);
        ''')

    for review in reviews:
        try:
            query = """
                INSERT INTO yelp_reviews (avatar, name, location, friends, reviews, photos, star, date, comment) VALUES
                ('{AVATAR}', '{NAME}', '{LOCATION}', '{FRIENDS}', '{REVIEWS}', '{PHOTOS}', '{STAR}', '{DATE}', '{COMMENT}');
                """.format(
                    AVATAR=review['avatar'], NAME=review['name'], LOCATION=review['location'],
                    FRIENDS=review['friends'], REVIEWS=review['reviews'], PHOTOS=review['photos'],
                    STAR=review['star'], DATE=review['date'], COMMENT=review['comment'].replace("'", "''"))
            if is_local:
                print(query)
            cur.execute(query)
            con.commit()
        except Exception as e:
            print(e)

    con.commit()
    con.close()


if __name__ == "__main__":

    try:
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        print("Current Time =", current_time)

        reviews = scrapper(is_local=True, business_name='studs-new-york')
        save2pg(is_local=True, reviews=reviews)

        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        print("Current Time =", current_time)
    except Exception as e:
        print(e)
