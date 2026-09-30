from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id")
w2 = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("shipping_fee", "id")

cleaned = (
    df_orders
    .filter(F.col("store_id").isNotNull() & (F.col("channel") == "store"))
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "shipping_fee",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

valid = (
    cleaned
    .filter((F.col("order_date") > F.lit("2023-07-06").cast("date")) & F.col("store_id").isNotNull())
    .select(
        "id",
        "order_date",
        "status",
        "shipping_fee",
        "channel_clean",
        F.initcap(F.trim("status")).alias("status_title"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean_2"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.first("shipping_fee", ignorenulls=True).over(w2).alias("first_known_shipping_fee"),
        F.ntile(3).over(w3).alias("shipping_fee_bucket"),
    )
)

base = (
    scored
    .filter(F.col("first_known_shipping_fee") < 30)
    .select(
        "id",
        "order_date",
        "shipping_fee",
        "channel_clean",
        "status_title",
        "channel_clean_2",
        "event_seq",
        (F.col("first_known_shipping_fee") * F.col("shipping_fee")).alias("first_known_shipping_fee_times_shipping_fee"),
    )
)

grouped = (
    base
    .groupBy(
        F.when(F.col("shipping_fee") >= 38, "HIGH").otherwise("LOW").alias("shipping_fee_tier"),
        F.date_trunc("YEAR", "order_date").alias("year_start"),
        F.date_trunc("WEEK", "order_date").alias("week_start"),
    )
    .agg(
        F.min("event_seq").alias("min_event_seq"),
        F.round(F.avg("first_known_shipping_fee_times_shipping_fee"), 2).alias("avg_first_known_shipping_fee_times_shipping_fee"),
        F.min("shipping_fee").alias("min_shipping_fee"),
    )
)

result = (
    grouped
    .select(
        "shipping_fee_tier",
        "year_start",
        "week_start",
        "min_event_seq",
        "avg_first_known_shipping_fee_times_shipping_fee",
        "min_shipping_fee",
    )
)
