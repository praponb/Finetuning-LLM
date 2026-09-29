from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.year("opened_date") == 2023)
    .groupBy("region")
    .agg(
        F.countDistinct("sqft").alias("unique_citys"),
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
    )
    .filter(F.col("unique_citys") < 9)
    .select("region", "unique_citys", "east_sqft")
)
