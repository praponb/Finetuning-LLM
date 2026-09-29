from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_times_shipping_fee").desc_nulls_first())
w2 = Window.partitionBy("store_id")
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.min("units").alias("min_store_sale_units"))

filtered = (
    df_orders
    .filter(F.col("channel").isNotNull() & (F.col("status") == "CANCELLED"))
    .select(
        "*",
        (F.col("discount") * F.col("shipping_fee")).alias("discount_times_shipping_fee"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

cleaned = (
    filtered
    .filter((F.col("order_date") > F.lit("2023-05-27").cast("date")) & F.col("store_id").isNotNull() & F.col("store_id").isin([6, 12, 22]))
    .select(
        "id",
        "store_id",
        "order_date",
        "channel",
        "discount",
        "shipping_fee",
        "discount_times_shipping_fee",
        "resolved_channel",
        F.trim("status").alias("clean_status"),
        F.trim("status").alias("clean_status_2"),
    )
)

matched_keys = (
    cleaned
    .select("id", "channel")
    .exceptAll(
        df_orders
        .filter(F.col("discount") <= 0.37)
        .select("id", "channel")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .withColumn("discount_times_shipping_fee_rank", F.rank().over(w))
    .withColumn("corr_shipping_fee_discount", F.corr("shipping_fee", "discount").over(w2))
)

kept_keys = (
    metrics
    .select("store_id", "channel")
    .intersectAll(
        df_orders
        .filter(F.col("amount") >= 1320)
        .select("store_id", "channel")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "discount",
        "discount_times_shipping_fee",
        "clean_status",
        "clean_status_2",
        "discount_times_shipping_fee_rank",
        F.col("min_store_sale_units"),
    )
)
