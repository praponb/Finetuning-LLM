from pyspark.sql import functions as F

result = (
    df_stores
    .filter(((F.col("region") != "North") & F.month("opened_date").isin([5, 7, 9])) | (F.col("sqft") != 14293))
    .groupBy("region")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("region") == "West", 1)).alias("west_count"),
    )
)
