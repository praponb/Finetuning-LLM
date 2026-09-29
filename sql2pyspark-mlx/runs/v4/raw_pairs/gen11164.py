from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "East")
    .cube(F.year("opened_date").alias("opened_year"), "region")
    .agg(F.sum(F.when(F.col("region") == "South", 1).otherwise(0)).alias("south_count"))
)
