from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("sqft", "id")
w3 = Window.orderBy(F.col("sqft").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-06-18").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "store_name",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.row_number().over(w2).alias("event_seq"),
        F.percent_rank().over(w3).alias("sqft_percent_rank"),
    )
)

valid = (
    enriched
    .filter(F.col("event_seq") >= 8)
    .select(
        "id",
        "store_name",
        "event_seq",
        "sqft_percent_rank",
        F.round("running_sqft", 1).alias("running_sqft_rounded"),
        F.substring("store_name", 1, 2).alias("store_name_prefix"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount"), F.col("c.shipping_fee"))
)

totals = (
    with_last
    .groupBy(
        F.when(F.col("running_sqft_rounded") >= 65, "HIGH").otherwise("LOW").alias("running_sqft_rounded_tier"),
        F.when(F.col("event_seq") >= 8, "LARGE").otherwise("SMALL").alias("event_seq_tier"),
        F.when(F.col("sqft_percent_rank") >= 0.68, "PREMIUM").otherwise("STANDARD").alias("sqft_percent_rank_tier"),
    )
    .agg(
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("max_shipping_fee") < 1000000)
)

result = (
    totals
    .select(
        "running_sqft_rounded_tier",
        "event_seq_tier",
        "sqft_percent_rank_tier",
        "max_shipping_fee",
    )
    .orderBy("max_shipping_fee", "running_sqft_rounded_tier")
)
