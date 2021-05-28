import requests
import json
import pandas as pd
import googlemaps
import time
import boto3

class SourseGatherer():

    def __init__(self, main_api):
        self.url = main_api


    def sourse_json2df(self):
        """
        This function reads the json file to find target sourses.
        """
        # Read Json
        response = requests.get(self.url)

        # dealing with the UTF-8 BOM error
        # https://www.howtosolutions.net/2019/04/python-fixing-unexpected-utf-8-bom-error-when-loading-json-data/
        decoded_data = response.text.encode().decode('utf-8-sig')
        data = json.loads(decoded_data)

        return data

    def sourse_namelist(self, dfdata):
        """
        It sets the sourses name into list.
        """

        name = []
        for i in range(len(dfdata['XML_Head']['Infos']['Info'])):
            name.append(dfdata['XML_Head']['Infos']['Info'][i]['Name'])

        return name

class StoresFromGoogle():

    def __init__(self, sourse_names, API_KEY, radius):
        self.spots = sourse_names

        # Google Place API_KEY & Search INFO
        self.API_KEY = API_KEY
        self.radius = radius

        # Google map API Place Type
        self.type = 'restaurant'


    def thr_googleapi(self):
        """
        From the sourses names, googleapi is applied to find the nearby restaurants.
        All the restaurants are stored into stores_info_basic dataframe.

        """
        # load Google API Key
        gmaps = googlemaps.Client(key=self.API_KEY)

        # Finding Restaurants From Enter places
        stores_info_basic = pd.DataFrame()
        for spot in self.spots:
            geocode_result = gmaps.geocode(spot)
            loc = geocode_result[0]['geometry']['location']
            if loc:
                places_result = gmaps.places_nearby(location=loc, radius=self.radius, type=self.type)
                if len(places_result['results']) == 20:
                    time.sleep(3)
                    places_result2 = gmaps.places_nearby(page_token=places_result['next_page_token'])

                    if len(places_result2['results']) == 20:
                        time.sleep(3)
                        places_result3 = gmaps.places_nearby(page_token=places_result2['next_page_token'])
                        df = pd.DataFrame.from_dict(
                            places_result['results'] + places_result2['results'] + places_result3['results'])
                        stores_info_basic = pd.concat([stores_info_basic, df])
                    else:
                        df = pd.DataFrame.from_dict(places_result['results'] + places_result2['results'])
                        stores_info_basic = pd.concat([stores_info_basic, df])
                else:
                    df = pd.DataFrame.from_dict(places_result['results'])
                    stores_info_basic = pd.concat([stores_info_basic, df])

        # Obtain the needed columns
        cols = ['geometry', 'name', 'place_id', 'price_level', 'rating', 'types', 'user_ratings_total',
                "permanently_closed"]
        stores_info_basic = stores_info_basic[cols]
        stores_info_basic.permanently_closed = stores_info_basic.permanently_closed.astype(str)

        return stores_info_basic

class StoresInfo2S3():

    def __init__(self, data, AWS_KEY_ID, AWS_SECRET, SaveName):
        self.data = data
        self.region_name = 'ap-northeast-1'
        self.aws_access_key_id = AWS_KEY_ID,
        self.aws_secret_access_key = AWS_SECRET
        self.LocalPath = 'D:\Dropbox\Work_Code\Project_GooglePlaces'
        self.SaveName = SaveName

    def store_info2S3(self):
        """
        Already creat a bucket called 'project-api'.

        bucket = S3.create_bucket(Bucket='project-api',
                                  CreateBucketConfiguration={
                                      'LocationConstraint': 'ap-northeast-1'}
                                  )
        """
        # Connent to S3
        S3 = boto3.client('s3',
                          region_name=self.region_name,
                          aws_access_key_id=self.aws_access_key_id,
                          aws_secret_access_key=self.aws_secret_access_key
                          )

        # Save Data to LocalPath
        Path = self.LocalPath + '\\' + self.SaveName
        self.data.to_csv(Path, index=True, encoding='utf_8_sig')

        # Upload data to S3
        S3.upload_file(
            Filename=Path,
            Bucket='project-api',
            Key=self.SaveName
        )

if __name__ == '__main__':
    # Place Json [景點, 餐飲點位, 民宿, 台南餐飲]
    main_apis = ['https://gis.taiwan.net.tw/XMLReleaseALL_public/scenic_spot_C_f.json',
                'https://gis.taiwan.net.tw/XMLReleaseALL_public/restaurant_C_f.json',
                'https://gis.taiwan.net.tw/XMLReleaseALL_public/hotel_C_f.json',
                'https://www.twtainan.net/data/shops_zh-tw.json']
    # SaveName
    save_names = ['Spots.csv', 'Rest.csv', 'Hotels.csv', 'TNRest.csv']

    # Google API KEY & AWS KEY
    API_KEY =
    AWS_KEY_ID =
    AWS_SECRET =


    for main_api in main_apis:
        for save_name in save_names:
            # Read Json Into DF
            s1 = SourseGatherer(main_api)
            data = s1.sourse_json2df()
            name = s1.sourse_namelist(data)

            # Search Near By Resturants
            GS = StoresFromGoogle(name, API_KEY, 1500)
            stores_info = GS.thr_googleapi()

            # Store Info Into AWS S3
            SIS3 = StoresInfo2S3(stores_info, AWS_KEY_ID, AWS_SECRET, save_name)
            SIS3.store_info2S3()
