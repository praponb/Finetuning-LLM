from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~(F.col("region").isNull() & (~F.col("city").isin(["Tokyo", "London", "New York"]) | F.col("region").isin(["North", "East", "West"]))))
    .withColumn("region_lower", F.lower("region"))
    .orderBy(F.col("city").desc(), "region")
)
