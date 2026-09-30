from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("country").isin(["SG", "US"]) & F.col("country").isin(["UK", "JP", "DE"]))
)

agg = (
    base
    .groupBy("segment", F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.count("*").alias("cnt"),
        F.min("age").alias("min_age"),
    )
)

result = (
    agg
    .filter(F.col("min_age") > 48)
    .select("segment", "signup_quarter", "cnt", "min_age")
    .orderBy("segment")
)
