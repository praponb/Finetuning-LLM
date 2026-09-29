from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("week_start").orderBy(F.col("tokyo_count").desc())

valid = (
    df_stores
    .filter(F.col("sqft") != 9520)
    .select(
        "*",
        F.date_sub("opened_date", 35).alias("opened_date_minus_35d"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

valid_stores = (
    valid
    .filter((F.col("opened_date") >= F.lit("2023-03-14").cast("date")) & F.col("region").isNotNull() & F.col("store_name").like("R%"))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        "opened_date_minus_35d",
        "resolved_region",
        F.length("city").alias("city_length"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region_2"),
    )
)

latest_match = (
    valid_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipping_fee"))
)

cleaned = (
    latest_match
    .filter(F.col("opened_date") > F.lit("2023-07-05").cast("date"))
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "opened_date_minus_35d",
        "resolved_region",
        "resolved_region_2",
        "shipping_fee",
        (F.col("sqft") * F.col("shipping_fee")).alias("sqft_times_shipping_fee"),
    )
)

result = (
    cleaned
    .groupBy(
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
        F.when(F.col("sqft") >= 16059, "LARGE").otherwise("SMALL").alias("sqft_tier"),
        "city",
    )
    .agg(
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.round(F.sum("sqft_times_shipping_fee") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("sqft_times_shipping_fee_per_chiang_mai"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("tokyo_count") > 1)
    .withColumn("tokyo_count_dense_rank", F.dense_rank().over(w2))
)
