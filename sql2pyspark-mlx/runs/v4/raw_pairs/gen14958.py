from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "channel").orderBy("discount", "id")

filtered = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-06-13").cast("date")) & F.col("store_id").isNotNull())
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.weekofyear("order_date").alias("order_date_weekofyear"),
    )
)

latest = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

cleaned = (
    latest
    .filter((F.col("order_date") >= F.lit("2023-12-29").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "order_date_weekofyear",
        F.year("order_date").alias("order_date_year"),
        F.upper("channel").alias("channel_upper"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

totals = (
    cleaned
    .groupBy(
        F.when(F.col("amount") >= 1630, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("order_date_weekofyear").alias("max_order_date_weekofyear"),
        F.corr("shipping_fee", "discount").alias("corr_shipping_fee_discount"),
    )
)

result = (
    totals
    .select("amount_tier", "max_order_date_weekofyear")
    .orderBy(F.col("amount_tier").asc_nulls_last(), "max_order_date_weekofyear")
)
