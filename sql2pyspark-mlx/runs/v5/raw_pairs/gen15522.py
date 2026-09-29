from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_last(), "id")

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        F.lower("region").alias("region_lower"),
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

cleaned_stores = (
    cleaned
    .filter(F.col("sqft") != 12453)
    .withColumn("sqft_plus_days_since_opened_date", (F.col("sqft") + F.col("days_since_opened_date")))
)

deduped = (
    cleaned_stores
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

valid = (
    deduped
    .filter((F.col("opened_date") > F.lit("2023-04-14").cast("date")) & (F.col("opened_date") > "2024-11-01"))
    .select(
        "id",
        "city",
        "sqft",
        "region_lower",
        "days_since_opened_date",
        (F.col("days_since_opened_date") + F.col("sqft_plus_days_since_opened_date")).alias("days_since_opened_date_plus_sqft_plus_days_since_opened_date"),
        F.when(F.col("sqft") >= 16057, "High").when(F.col("sqft") >= 3920, "Medium").otherwise("Low").alias("sqft_level"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("days_since_opened_date") >= 80, "PREMIUM").otherwise("STANDARD").alias("days_since_opened_date_tier"),
        F.when(F.col("sqft") >= 14751, "LARGE").otherwise("SMALL").alias("sqft_tier"),
    )
    .agg(
        F.round(F.sum("days_since_opened_date_plus_sqft_plus_days_since_opened_date") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("days_since_opened_date_plus_sqft_plus_days_since_opened_date_per_chiang_mai"),
        F.percentile_approx("sqft", 0.95).alias("sqft_p95"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.max(F.when(F.col("city") == "New York", F.col("days_since_opened_date"))).alias("new_york_max_days_since_opened_date"),
    )
)
