from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StringType,IntegralType,StringType





def create_spark_session():
    return SparkSession.builder.appName('Basics').getOrCreate()

def create_file_df(filepath,sc):
    return sc.read.option("header", "true").csv(filepath)

def main():
    sc = create_spark_session()
    filepath ='/home/riyazaamir/udemy/training/MOCK_DATA.csv'
    df = create_file_df(filepath,sc)
    












if __name__== "__main__":
    main()
