from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(6533, 8449) & (F.col("city") == "London") & (F.col("region") == "South"))
    .groupBy("city", "region")
    .agg(
        F.sum(F.when(F.col("region") == "South", F.col("sqft")).otherwise(0)).alias("south_sqft"),
    )
    .filter(F.col("south_sqft") < 68)
)
