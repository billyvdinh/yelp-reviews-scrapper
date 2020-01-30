import os
import time
import requests
from datetime import datetime
import random
from lxml import html
import psycopg2

max_per_page = 20


def handler(event, context):
    try:
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        print("Started at =", current_time)

        reviews = grab_data('studs-new-york')
        save2pg(reviews)

        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        print("Ended at =", current_time)
    except Exception as e:
        print(e)


def grab_data(name='first-bay-locksmith-santa-clara-3'):
    reviews_list = []
    url = 'https://www.yelp.com/biz/' + name
    random_delay = random.random()
    time.sleep(random.randint(5, 10) + random_delay)
    page = requests.get(url)
    parser = html.fromstring(page.content)

    str_num_reviews = parser.xpath('//p[@class="lemon--p__373c0__3Qnnj text__373c0__2pB8f '
                                   'text-color--mid__373c0__3G312 text-align--left__373c0__2pnx_ '
                                   'text-size--large__373c0__1568g"]//text()')
    num_reviews = 0
    if len(str_num_reviews) > 0:
        num_reviews = int(str_num_reviews[0].replace(" reviews", ""))
    ipage = 0
    while num_reviews > 0:
        random_delay = random.random()
        time.sleep(random.randint(2, 10) + random_delay)
        page = requests.get(url + '?start={}'.format(ipage * max_per_page))
        parser = html.fromstring(page.content)
        reviews = parser.xpath('//div[@class="lemon--div__373c0__1mboc '
                               'sidebarActionsHoverTarget__373c0__2kfhE arrange__373c0__UHqhV '
                               'gutter-12__373c0__3kguh grid__373c0__29zUk '
                               'layout-stack-small__373c0__3cHex border-color--default__373c0__2oFDT"]')

        for review in reviews:
            user_img_src = review.xpath('.//img[@class="lemon--img__373c0__3GQUb photo-box-img__373c0__O0tbt"]/@src')[0]
            user_name = review.xpath('.//div[@class="lemon--div__373c0__1mboc user-passport-info '
                                     'border-color--default__373c0__2oFDT"]//span/text()')[0]
            user_addr = review.xpath('.//span[@class="lemon--span__373c0__3997G text__373c0__2pB8f '
                                     'text-color--normal__373c0__K_MKN text-align--left__373c0__2pnx_ '
                                     'text-weight--bold__373c0__3HYJa text-size--small__373c0__3SGMi"]/text()')[0]
            user_friends = review.xpath('.//span[@class="lemon--span__373c0__3997G"]//b/text()')[0]
            user_reviews_num = review.xpath('.//span[@class="lemon--span__373c0__3997G"]//b/text()')[1]
            rating = review.xpath('.//div[@class="lemon--div__373c0__1mboc arrange-unit__373c0__1piwO '
                                  'border-color--default__373c0__2oFDT"]//span//div/'
                                  '@aria-label')[0].replace(" star rating", "")
            date = review.xpath('.//div[@class="lemon--div__373c0__1mboc arrange-unit__373c0__1piwO '
                                'arrange-unit-fill__373c0__17z0h border-color--default__373c0__2oFDT"]//span/text()')[0]
            comment = review.xpath('.//p[@class="lemon--p__373c0__3Qnnj text__373c0__2pB8f comment__373c0__3EKjH '
                                   'text-color--normal__373c0__K_MKN text-align--left__373c0__2pnx_"]//span//text()')[0]

            review_json = {
                'AVATAR': user_img_src,
                'NAME': user_name,
                'ADDR': user_addr,
                'FRIENDS': int(user_friends),
                'REVIEWS': int(user_reviews_num),
                'RATING': int(rating),
                'DATE': date,
                'COMMENT': comment
            }
            reviews_list.append(review_json)

        num_reviews -= max_per_page
        print('++{}+++'.format(ipage))
        ipage += 1

        # Test Mode
    return reviews_list


def save2pg(reviews):
    con = psycopg2.connect(
        host=os.environ['REDSHIFT_HOST'],
        port=os.environ['REDSHIFT_PORT'],
        database=os.environ['REDSHIFT_DBNAME'],
        user=os.environ['REDSHIFT_USERNAME'],
        password=os.environ['REDSHIFT_PASSWORD']
    )

    # Drop Table If Exists...
    cur = con.cursor()
    cur.execute('''DROP TABLE IF EXISTS yelp_reviews;''')
    con.commit()

    cur = con.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS yelp_reviews(
                    AVATAR        VARCHAR(256)      NOT NULL,
                    NAME          VARCHAR(256)      NOT NULL,
                    ADDR          VARCHAR(256)      NOT NULL,
                    FRIENDS       INT               NOT NULL,
                    REVIEWS       INT               NOT NULL,
                    RATING        INT               NOT NULL,
                    DATE          VARCHAR(20),
                    COMMENT       VARCHAR(2000)     NOT NULL
                );
        ''')

    for review in reviews:
        cmt = review['COMMENT'].replace("'", "''")
        cur.execute(
            """INSERT INTO yelp_reviews (AVATAR, NAME, ADDR, FRIENDS, REVIEWS, RATING, DATE, COMMENT)
            VALUES ('{0}', '{1}', '{2}', {3}, {4}, {5}, '{6}', '{7}')"""
            .format(review['AVATAR'], review['NAME'], review['ADDR'], review['FRIENDS'], review['REVIEWS'],
                    review['RATING'], review['DATE'], cmt));

        con.commit()

    con.commit()
    con.close()


if __name__ == "__main__":

    try:
        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        print("Current Time =", current_time)

        reviews = grab_data('studs-new-york')
        save2pg(reviews)

        now = datetime.now()
        current_time = now.strftime("%d/%m/%Y %H:%M:%S")
        print("Current Time =", current_time)
    except Exception as e:
        print(e)
