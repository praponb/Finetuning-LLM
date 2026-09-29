from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-09-25").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.when(F.col("sqft") >= 17098, "High").when(F.col("sqft") >= 4760, "Medium").otherwise("Low").alias("sqft_level"),
        F.dayofmonth("opened_date").alias("opened_date_day"),
    )
)

result = (
    base
    .withColumn("year_start", F.date_trunc("YEAR", "opened_date"))
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 13041, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["year_start", "sqft_tier"], ["year_start"], []],
        "year_start",
        "sqft_tier",
    )
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.countDistinct("region").alias("distinct_regions"),
        F.round(F.avg("opened_date_day"), 2).alias("avg_opened_date_day"),
        F.grouping_id().alias("gid"),
    )
)
