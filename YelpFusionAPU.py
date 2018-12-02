import requests  #pip install
from avro import schema, datafile, io #pip install
import json  #Built-in
from urllib.parse import quote  #Built-in
import pathlib #Built-in
from time import localtime, strftime  #Built-in

#GLOBAL VARS
record_count = 200

city_list = ['Chicago, IL',
             'New York, NY',
             'Los Angeles, CA',
             'Las Vegas, NV',
             'Honolulu, HI',
             'Miami, FL',
             'New Orleans, LA',
             'Boston, MA',
             'Nashville, TN',
             'Houston, TX',
             'Denver, CO',
             'Seattle, WA',
             'Phoenix, AZ',
             'Atlanta, GA',
             'Dallas, TX',
             'San Francisco, CA',
             'Kansas City, KS',
             'Washington, DC',
             'Philadelphia, PA',
             'Portland, OR'
             ]

config_path = str(pathlib.Path.cwd())
config_name = "config.json"
config_file = config_path + "/" + config_name
#print(config_file)


snapshot_date_local = localtime()
snapshot_date = strftime("%Y-%m-%d", snapshot_date_local)
snapshotDateStringFile = strftime("%Y%m%d_%H%M%S", snapshot_date_local)

def yelp_fusion_business_search(search_limit, offset, location):
    api_key = API_KEY
    host = 'https://api.yelp.com'
    path = '/v3/businesses/search'

    # Defaults for our simple example.
    term = 'restaurants'
    sort_by = 'review_count'  # review_count, distance

    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': search_limit,
        'sort_by': sort_by,
        'offset': offset
    }

    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    response = requests.request('GET', url, headers=headers, params=url_params)
    return response


#AVRO
with open(config_file, "r+") as fo:
    schema_str = fo.read();
    parsed_json = json.loads(schema_str)
    avro_schema_file = parsed_json["avro_schema"]
    avro_output_file = parsed_json["avro_output"]


outfile_name = avro_output_file+'yelp_fusion_bizs_'+snapshotDateStringFile+'.avro'
outfile_name2 = r'/home/rjz/Data/yelp_bizsearc_api3.avro'
#input_schema_name = r"/home/rjz/config/yelp_bizsearc_api.avsc"

# Open a file
fa = open(avro_schema_file, "r+")
schema_str = fa.read();
#print("Read String is : ", schema_str)
fa.close()
fo.close()

SCHEMA = schema.Parse(schema_str)


# Create a 'record' (datum) writer
rec_writer = io.DatumWriter(SCHEMA)

# Create a 'data file' (avro file) writer
df_writer = datafile.DataFileWriter(
    open(outfile_name, 'wb'),
    rec_writer,
    writer_schema=SCHEMA
)


offset = 0
while offset <= record_count:
    for city in city_list:
        yelp_output = yelp_fusion_business_search(50, offset, city)
        json_string = yelp_output.text
        parsed_json = json.loads(json_string)
        results = parsed_json["businesses"]
        for result in results:
            raw_url = result["url"]
            raw_id = result["id"]
            raw_display_phone = result['display_phone']
            raw_review_count = result["review_count"]
            raw_rating = result["rating"]
            raw_categories = result["categories"]
            raw_coordinates = result['coordinates']
            raw_alias = result["alias"]
            raw_transactions = result["transactions"]
            raw_name = result["name"]
            raw_is_closed = result["is_closed"]
            raw_location = result["location"]
            raw_image_url = result['image_url']
            raw_price = result["price"]
            raw_distance = result["distance"]
            raw_phone = result["phone"]
            if raw_categories:
                raw_categories_first_title = raw_categories[0]["title"]
                raw_categories_first_alias = raw_categories[0]["alias"]
            else:
                raw_categories_first_title = 'UNKNOWN'
                raw_categories_first_alias = 'UNKNOWN'

            df_writer.append({"snapshot_date": snapshot_date,
                              "search_location": city,
                              "url": raw_url,
                              "id": raw_id,
                              "display_phone": raw_display_phone,
                              "review_count": raw_review_count,
                              "rating": raw_rating,
                              "coordinates": raw_coordinates,
                              "coordinates.latitude": raw_coordinates["latitude"],
                              "coordinates.longitude": raw_coordinates["longitude"],
                              "alias": raw_alias,
                              "transactions": raw_transactions,
                              "name": raw_name,
                              "is_closed": raw_is_closed,
                              "location.address1": raw_location["address1"],
                              "location.address2": raw_location["address2"],
                              "location.address3": raw_location["address3"],
                              "location.country": raw_location["country"],
                              "location.city": raw_location["city"],
                              "location.state": raw_location["state"],
                              "location.zip_code": raw_location["zip_code"],
                              "location.display_address": raw_location["display_address"],
                              "image_url": raw_image_url,
                              "price": raw_price,
                              "distance": raw_distance,
                              "phone": raw_phone,
                              "categories": raw_categories,
                              "categories.first_title": raw_categories_first_title,
                              "categories.first_alias": raw_categories_first_alias
                              })

    offset = offset + 50
df_writer.close()

