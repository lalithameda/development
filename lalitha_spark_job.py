# Importing required packages
from pyspark.sql.types import DecimalType,StringType
from pyspark.sql import functions as f
import json
import sys
from pyspark.sql import SparkSession 
from pyspark.conf import SparkConf
import pyspark.sql.utils 
from pyspark.sql.utils import AnalysisException

# Initialising Spark 
spark = SparkSession.builder.appName('Data_Transformation').getOrCreate()

# Loading Delta from Jar file in S3
delta_jar = sys.argv[2]
spark.sparkContext.addPyFile(delta_jar)
#spark.sparkContext.addPyFile("s3://lalitha-landingzone/files/delta-core_2.12-0.8.0.jar")

# Importing delta
from delta import *

class Data_Transformation():
    
    # The __init__ function is called every time an object is created from a class
    def __init__(self):
    
        # reading app-config file
        self.jsonData=self.read_config()        
        
        # Assigning variables for app_configuration File Parameters        
        self.ingest_datasets = self.jsonData['datasets']
        self.ingest_actives_source = self.jsonData['ingest-Actives']['source']['data-location']
        self.ingest_actives_destination = self.jsonData['ingest-Actives']['destination']['data-location']
        self.ingest_viewership_source = self.jsonData['ingest-Viewership']['source']['data-location']
        self.ingest_viewership_destination = self.jsonData['ingest-Viewership']['destination']['data-location']
        self.transformation_cols_actives = self.jsonData['masked-Actives']['transformation-cols']
        self.transformation_cols_viewership = self.jsonData['masked-Viewership']['transformation-cols']
        self.ingest_raw_actives_source = self.jsonData['masked-Actives']['source']['data-location']
        self.ingest_raw_viewership_source = self.jsonData['masked-Viewership']['source']['data-location']
        self.ingest_raw_actives_Destination = self.jsonData['masked-Actives']['destination']['data-location']
        self.ingest_raw_viewership_Destination = self.jsonData['masked-Viewership']['destination']['data-location']
        self.masking_col_actives= self.jsonData['masked-Actives']['masking-cols']
        self.masking_col_viewership= self.jsonData['masked-Viewership']['masking-cols']        
        self.partition_col_actives= self.jsonData['masked-Actives']['partition-cols']
        self.partition_col_viewership= self.jsonData['masked-Viewership']['partition-cols']
        self.lookup_location = self.jsonData['lookup-dataset']['data-location']
        self.pii_cols =  self.jsonData['lookup-dataset']['pii-cols']
        
        
                      
    #Function to read app_configuration file
    def read_config(self):
        app_config = spark.conf.get("spark.path")
        configData = spark.sparkContext.textFile(app_config).collect()
        data       = ''.join(configData)
        jsonData = json.loads(data)
        return jsonData
        
    #Function to read file from s3 Location
    def read_data(self, path):
        df = spark.read.parquet(path)
        return df
    
    #Function to write file to s3 Location
    def write_data(self, df, path, partition_cols = []):
        if partition_cols:
            df.write.mode("overwrite").partitionBy(partition_cols[0], partition_cols[1]).parquet(path)
        else:
            df.write.mode("overwrite").parquet(path)
        
    #Function to mask PII columns in a file
    def mask_data(self, df, column_list):
        for column in column_list:
            #df = df.withColumn("masked_"+column,f.concat(f.lit('***'),f.substring(f.col(column),4,3)))
            df = df.withColumn("masked_"+column,f.sha2(f.col(column),256))
        return df
    
    #Function to casting required fileds in a file
    def cast_data(self, df, cast_dict):
        for key in cast_dict.keys(): 
            if cast_dict[key].split(",")[0] == "DecimalType":
                df = df.withColumn(key, df[key].cast(DecimalType(10, int(cast_dict[key].split(",")[1])))) 
            elif cast_dict[key] == "ArrayType-StringType":
                df.withColumn(key,f.concat_ws(",",f.col(key)))
        return df
      
    # scd2 type implementation
    def scd2_implementation(self, df, lookup_location, pii_cols, dataset):        
        file_name = dataset
        # Adding start_date and end_date columns to df
        df_source = df.withColumn("start_date",f.current_date())
        df_source = df_source.withColumn("end_date",f.lit("null"))        
          
        # getting required (masked and unmasked pii columns) from a dataset 
        columns_needed = []
        for col in pii_cols:
            if col in df.columns:
                columns_needed += [col,"masked_"+col]
        
        # adding start_date, end_date columns 
        source_columns_used = columns_needed + ['start_date','end_date']        
        df_source = df_source.select(*source_columns_used)
        
        # Checking if Delta Table exists 
        try:
            # Reading data from existing Delta table
            targetTable = DeltaTable.forPath(spark, lookup_location + dataset)
            # Converting Delta Table to DF
            delta_df = targetTable.toDF()
        except pyspark.sql.utils.AnalysisException:
            print('Table does not exist')
            # Creating delta table if not exists
            df_source = df_source.withColumn("flag_active",f.lit("true"))
            df_source.write.format("delta").mode("overwrite").save(lookup_location + dataset)
            print('Table Created Sucessfully!')
            # Retrieving Delta Table
            targetTable = DeltaTable.forPath(spark, lookup_location + dataset)
            # Converting Delta Table to DF
            delta_df = targetTable.toDF()
            delta_df.show(10)
            
        # renaming delta_df columns names  with target_+ columnname    
        delta_df = delta_df.select(*(f.col(i).alias('target_' + i) for i in delta_df.columns))        
        
        join_condition = [df_source["advertising_id"] == delta_df["target_advertising_id"]]
        # perform left join on df_source, delta_df to fetch matched and un-matched data
        join_df = df_source.join(delta_df, join_condition, "leftouter").select(df_source['*'], delta_df['*'])        

        #fetching only new records from source
        new_records = join_df.filter("target_user_id is null")
        #fetching records with updated values from source
        updated_records = join_df.filter(join_df.user_id != join_df.target_user_id)
        # combining new & updated records from source
        filter_df = updated_records.union(new_records)

        # if any new updates/records exists:
        if filter_df.count() != 0:
            #Adding new column with MERGEKEY
            mergeDf = filter_df.withColumn("MERGEKEY", f.concat(filter_df.advertising_id, filter_df.target_user_id))
            #Adding values to new Mergkey column
            dummyDf = filter_df.filter("target_advertising_id is not null").withColumn("MERGEKEY", f.lit(None))
            # stagedUpdates is union of mergeDf and dummyDf
            stagedUpdates = mergeDf.union(dummyDf)
            
            # creating dictionary to add new record details
            Insertable = {i: "source." + i for i in columns_needed}
            # Adding flag_active, start_date, end_date columns for new records dictionary 
            Insertable.update({"start_date": "current_date", "end_date": "null", "flag_active": "True"})
            
            # Appling SCD Type 2 Operation using Merge
            targetTable.alias("target").merge(
                source=stagedUpdates.alias("source"),
                condition="concat(target.advertising_id, target.user_id) = source.MERGEKEY and target.flag_active = 'true'"
            ).whenMatchedUpdate(set={
            # Set flag_active as false and end_date as new record's start date.
                "end_date": "current_date",
                "flag_active": "False",
            }).whenNotMatchedInsert(values=Insertable
                                    ).execute()
            
        for i in pii_cols:
            df = df.drop(i).withColumnRenamed("masked_" + i, i)

        return df


# Creating an object for Data_Transformation class
T = Data_Transformation()
dataset = sys.argv[1]


if dataset == "Actives": 
    # read actives files from Landing Zone
    actives_landing_data = T.read_data(T.ingest_actives_source)
    # write actives files from Landing Zone to Raw Zone
    T.write_data(actives_landing_data, T.ingest_actives_destination)
    # read actives files from Raw Zone
    actives_raw_data = T.read_data(T.ingest_raw_actives_source)
    # masking required fields from actives files
    actives_masked_data = T.mask_data(actives_raw_data, T.masking_col_actives)
    # casting required fields from actives files
    actives_tranform_data = T.cast_data(actives_masked_data, T.transformation_cols_actives)        
    # scd2 implementation
    lookup_data = T.scd2_implementation(actives_tranform_data, T.lookup_location, T.pii_cols, dataset)
    # write actives Transformed data to Staging Zone
    T.write_data(lookup_data, T.ingest_raw_actives_Destination, T.partition_col_actives)
    
elif dataset == "Viewership":
    # read viwership files from Landing Zone
    viewership_landing_data = T.read_data(T.ingest_viewership_source) 
    # write viwership files from Landing Zone to Raw Zone
    T.write_data(viewership_landing_data, T.ingest_viewership_destination) 
    # read viwership files from Raw Zone
    viewership_raw_data = T.read_data(T.ingest_raw_viewership_source)
    # masking required fields from viwership files
    viewership_masked_data = T.mask_data(viewership_raw_data, T.masking_col_viewership)   
    # casting required fields from viwership files
    viewership_transform_data = T.cast_data(viewership_masked_data, T.transformation_cols_viewership)
    # scd2 implementation
    #lookup_data = T.scd2_implementation(actives_tranform_data, T.lookup_location, T.pii_cols, dataset)        
    # write viwership Transformed data to Staging Zone
    T.write_data(lookup_data, T.ingest_raw_viewership_Destination, T.partition_col_viewership)
    
else:
    pass
