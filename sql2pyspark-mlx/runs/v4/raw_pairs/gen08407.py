from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Consumer", "SMB"]))
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.max("signup_date").alias("latest_signup_date"),
    )
    .orderBy(F.col("signup_quarter").desc())
)
