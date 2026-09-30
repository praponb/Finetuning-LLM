from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(9463, 12860) & (F.col("region") == "West") & (F.year("opened_date") == 2025))
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
    .withColumn("is_west", F.when(F.col("region") == "West", 1).otherwise(0))
    .orderBy("store_name", "id")
    .limit(10)
)
