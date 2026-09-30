from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "channel")
w2 = Window.partitionBy("status")
w3 = Window.orderBy(F.col("shipping_fee").desc_nulls_first())
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.sum("units").alias("sum_store_sale_units"))

remaining_keys = (
    df_orders
    .select("status", "channel")
    .subtract(
        df_orders
        .filter(F.year("order_date") == 2023)
        .select("status", "channel")
    )
)

kept_rows = (
    df_orders
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .filter(F.col("channel") == "store")
    .select(
        "*",
        F.corr("amount", "shipping_fee").over(w).alias("corr_amount_shipping_fee"),
        F.corr("shipping_fee", "amount").over(w2).alias("corr_shipping_fee_amount"),
        F.percent_rank().over(w3).alias("shipping_fee_percent_rank"),
    )
)

filtered = (
    scored
    .filter(F.col("shipping_fee").between(11, 12))
    .select(
        "*",
        (F.col("corr_shipping_fee_amount") / F.nullif("shipping_fee_percent_rank", F.lit(0))).alias("corr_shipping_fee_amount_per_shipping_fee_percent_rank"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

matched_keys = (
    filtered
    .select("channel")
    .subtract(
        df_orders
        .filter(F.col("discount") >= 0.34)
        .select("channel")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "corr_amount_shipping_fee",
        F.col("sum_store_sale_units"),
    )
    .orderBy(F.col("corr_amount_shipping_fee").desc_nulls_first())
)
