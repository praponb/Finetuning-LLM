from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-08-30").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.date_sub("opened_date", 77).alias("opened_date_minus_77d"),
        F.coalesce("region", F.lit("OTHER")).alias("resolved_region"),
    )
)

result = (
    filtered
    .groupBy("city")
    .pivot("region", ["East", "South"])
    .agg(F.sum("sqft").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
