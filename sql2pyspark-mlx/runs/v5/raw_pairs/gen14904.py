from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") == 6082) & (F.col("city") == "London"))
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
    )
)
