from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("city").isin(["New York", "Bangkok"]))
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("sqft")).otherwise(0)).alias("new_york_sqft"),
    )
    .filter(F.col("new_york_sqft") > 50)
    .orderBy("city")
)
