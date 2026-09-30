from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id").orderBy("discount")
w3 = Window.partitionBy("status", "store_id")

scored = (
    df_orders
    .select(
        "id",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        F.min("shipping_fee").over(w).alias("customer_id_min_shipping_fee"),
        F.percent_rank().over(w2).alias("discount_percent_rank"),
        F.corr("shipping_fee", "amount").over(w3).alias("corr_shipping_fee_amount"),
    )
)

valid = (
    scored
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        "customer_id_min_shipping_fee",
        "discount_percent_rank",
        F.initcap(F.trim("status")).alias("status_title"),
        F.col("shipping_fee").cast("int").alias("shipping_fee_int"),
    )
)

base = (
    valid
    .filter(F.col("customer_id_min_shipping_fee").isNull())
    .select(
        "id",
        "channel",
        "discount",
        "shipping_fee",
        "customer_id_min_shipping_fee",
        "discount_percent_rank",
        "status_title",
        "shipping_fee_int",
        F.when(F.col("discount") >= 0.4, "High").when(F.col("discount") >= 0.19, "Medium").otherwise("Low").alias("discount_band"),
    )
)

remaining_keys = (
    base
    .select("id", "channel")
    .subtract(
        df_orders
        .filter(F.col("store_id").isin([36, 39]))
        .select("id", "channel")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

kept = (
    verified
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.amount") < 710), "left_anti")
)

result = (
    kept
    .groupBy("channel")
    .agg(
        F.percentile_approx("shipping_fee_int", 0.5).alias("shipping_fee_int_p50"),
        F.corr("discount", "shipping_fee").alias("corr_discount_shipping_fee"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("discount_per_mobile"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
    )
    .orderBy(F.col("channel").asc_nulls_first())
)
