import requests
from bs4 import BeautifulSoup
import re
import datetime
from time import gmtime, strftime

def append_row(SnapshotDatetime, RestaurantName, StarRating, ReviewCount, Neightborhood):
    with open(r'C:\Data\YelpData.txt', 'a') as outFile:
        outFile.write(SnapshotDatetime + '|'+RestaurantName+'|'+StarRating+'|'+ReviewCount+'|'+Neightborhood + '\n')

r = requests.get('https://www.yelp.com/search?find_desc=restaurants&find_loc=Chicago%2C+IL&ns=1')
soup = BeautifulSoup(r.text, "html.parser")

divTableList = soup.find_all("ul", class_="ylist ylist-bordered search-results js-search-results yloca-pills-blue yloca-wrapper-grey")
divTableItem = divTableList[1]
divRows = divTableItem.find_all("li", class_="regular-search-result")
snapshotDate = gmtime()
snapshotDateString = strftime("%a, %d %b %Y %H:%M:%S", snapshotDate)

append_row('SNAPSHOT DATE', 'RESTAURANT NAME', 'STAR RATING', 'REVIEW COUNT', 'NEIGHBORHOOD')
for row in divRows:
    RestaurantName = (row.find("div", class_="main-attributes").a.span).string
    StarRating = (row.find("div", class_="main-attributes").find("div", class_=re.compile("i-stars i-stars"))['title'])
    ReviewCount = ((row.find("div", class_="main-attributes").find("span", class_="review-count rating-qualifier")).string).strip()
    Neighborhood = ((row.find("div", class_="secondary-attributes").find("span", class_="neighborhood-str-list")).string).strip()
    append_row(snapshotDateString, RestaurantName, StarRating, ReviewCount, Neighborhood)



