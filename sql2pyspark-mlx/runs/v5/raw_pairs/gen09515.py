from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~(F.col("plan").isin(["enterprise", "basic"]) & F.col("end_date").between("2023-10-16", "2025-10-01") & (F.col("start_date") > "2023-08-01")))
    .groupBy(F.year("start_date").alias("start_year"))
    .agg(
        F.min("start_date").alias("min_start_date"),
        F.max("start_date").alias("latest_start_date"),
    )
)
