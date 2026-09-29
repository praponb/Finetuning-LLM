from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.year("opened_date") >= 2023) & ((F.col("opened_date") >= "2023-08-01") | F.col("store_name").like("%Kiosk%")))
    .withColumn("opened_date_plus_21d", F.date_add("opened_date", 21))
    .orderBy("region", "city")
)
