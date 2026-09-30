from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("segment").isin(["Consumer", "Corporate"]))
    .groupBy("city")
    .agg(F.min("age").alias("min_age"))
    .filter(F.col("min_age") > 49)
    .orderBy(F.col("city").desc())
)
