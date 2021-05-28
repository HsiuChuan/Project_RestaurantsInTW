import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import pyspark.sql.functions as F
import pandas as pd
# import order must be right
from shapely.geometry import shape, Point
import geopandas
import fiona

class SparkJob():

    def __init__(self, AWS_KEY_ID, AWS_SECRET):
        # AWS
        self.AWS_KEY_ID = AWS_KEY_ID
        self.AWS_SECRET = AWS_SECRET
        self.region_name = 'ap-northeast-1'
        # Spark
        self.spark = SparkSession.builder \
            .config("spark.executor.memory", "5g") \
            .config("spark.driver.memory", "5g") \
            .getOrCreate()

    def read_csv(self):
        """
        Read All the Data in AWS S3 and Union Together.
        Return a DataFrame.
        """
        # Connect to S3 & Get the Bucket List
        S3 = boto3.client('s3',
                          region_name=self.region_name,
                          aws_access_key_id=self.AWS_KEY_ID,
                          aws_secret_access_key=self.AWS_SECRET
                          )
        DataLists = [i['Key'] for i in S3.list_objects(Bucket='project-api')['Contents']]

        # Spark Read S3 Settings
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", self.AWS_KEY_ID)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", self.AWS_SECRET)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        self.spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                                  "org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3-ap-northeast-1.amazonaws.com")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

        # Read All the Data & Union Together
        for id, DataList in enumerate(DataLists):
            path = 's3a://project-api/' + DataList
            print("Reading " + DataList)
            if id == 0:
                df = self.spark.read.csv(path, header=True)  # inferSchema=True
                dataF = df
            else:
                df = self.spark.read.csv(path, header=True)
                dataF = dataF.union(df)

        return dataF

    def clean_data(self, dataF):

        # Drop duplicates
        dataF = dataF.drop_duplicates(subset=['place_id'])

        # delete permanently_closed = True
        dataF = dataF.filter(dataF.permanently_closed == 'nan')
        dataF = dataF.drop("permanently_closed")

        # drop na in rating column
        dataF = dataF.withColumn('rating', dataF.rating.cast(FloatType()))
        dataF = dataF.na.drop(subset=["rating"])
        # dataF = dataF.filter(dataF.rating.isNotNull())

        # drop na in user_ratings_total column
        dataF = dataF.withColumn('user_ratings_total', dataF.user_ratings_total.cast(IntegerType()))
        dataF = dataF.na.drop(subset=["user_ratings_total"])

        # Fill 0 in nan row
        dataF = dataF.withColumn('price_level', dataF.price_level.cast(IntegerType()))
        dataF = dataF.withColumn('price_level',
                                 F.when(F.isnan(dataF.price_level), 0)
                                 .otherwise(dataF.price_level))
        return dataF

    def clean_data_udf(self, dataF):

        # Using UDF to deal with types
        def get_Types(data):
            data = data.replace("[", '').replace("]", '').split(', ')[:3]
            return data

        udf_get_Types = udf(get_Types, ArrayType(StringType()))
        dataF = dataF.withColumn('types', udf_get_Types(dataF.types))

        # split types
        dataF = dataF.withColumn("key1", dataF.types.getItem(0)) \
            .withColumn("key2", dataF.types.getItem(1)) \
            .withColumn("key3", dataF.types.getItem(2))
        dataF = dataF.drop("types")

        # UDF with geometry to get lat & lng
        # And drop geometry
        def get_lat(data):
            data = data.split('{', )[2].split('}')[0].split(',')[0].split(':')[1].strip()
            return float(data)

        udf_get_lat = udf(get_lat, FloatType())
        dataF = dataF.withColumn('lat', udf_get_lat(dataF.geometry))

        def get_lng(data):
            data = data.split('{')[2].split('}')[0].split(',')[1].split(":")[1].strip()
            return float(data)

        udf_get_lng = udf(get_lng, FloatType())
        dataF = dataF.withColumn('lng', udf_get_lng(dataF.geometry))
        dataF = dataF.drop("geometry")

        return dataF

    def exact_location(self, dataF):
        """
        Search For the exact location from the shapefile.
        """
        # Obtain the execat location of store
        collection = fiona.open('.\mapdata202104280242\VILLAGE_MOI_1100415.shp', encoding='utf-8')

        shapes = {}
        properties = {}
        for f in collection:
            vill_id = str(f['properties']['VILLCODE'])
            shapes[vill_id] = shape(f['geometry'])
            properties[vill_id] = f['properties']

        def search_location(x, y):
            # x = lng, y = lat
            global vill_id
            global shapes
            global properties
            try:
                kk = next((vill_id for vill_id in shapes if shapes[vill_id].contains(Point(x, y))), None)
                return properties[kk]['COUNTYNAME'] + properties[kk]['TOWNNAME'] + properties[kk]['VILLNAME']
            except KeyError:
                return None

        udf_search_location = udf(search_location, StringType())
        dataF = dataF.withColumn('related_place', udf_search_location(dataF.lng, dataF.lat))

        return dataF

class Data2S3():

    def __init__(self, localuser, localpassword):
        self.driver = 'com.mysql.jdbc.Driver'
        self.database = 'google_stores'
        self.dbtable = 'store_info'
        self.user = localuser
        self.password = localpassword
        self.url = 'jdbc:mysql://localhost:3306/' + self.database +\
                   '?useUnicode=true&characterEncoding=utf8&serverTimezone=GMT%2B8&useSSL=false'

    def write_2db(self, dataF):
        """
        Write Data to MySql From Spark.
        """
        # Final Columns Exported to MySql DB
        dataF = dataF.select("name", "place_id", "lat", "lng", 'price_level', 'rating',
                             'user_ratings_total', 'related_place', 'key1', 'key2', 'key3')

        # write out to database
        dataF.write.format('jdbc').options(
            url=self.url,
            driver=self.driver,
            dbtable=self.dbtable,
            user=self.user,
            password=self.password) \
            .mode('append') \
            .save()

if __name__ == '__main__':
    AWS_KEY_ID = ''
    AWS_SECRET = ''

    # Load Data From AWS S3 to Spark
    # Clean data in Spark
    SJ = SparkJob(AWS_KEY_ID, AWS_SECRET)
    dataF = SJ.read_csv()
    dataF = SJ.clean_data(dataF)
    dataF = SJ.clean_data_udf(dataF)
    dataF = SJ.exact_location(dataF)

    # Write Data to MySql From Spark
    localuser = ''
    localpassword = ''

    D2S = Data2S3(localuser, localpassword)
    D2S.write_2db(dataF)