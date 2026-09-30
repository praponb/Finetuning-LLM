from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("order_date").desc(), "id")

base = (
    df_orders
    .filter(~F.col("status").isin(["COMPLETED", "CANCELLED", "PENDING"]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.round("discount", 0).alias("discount_rounded"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    latest
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.credit_limit"))
)

stats = (
    joined
    .groupBy(
        "store_id",
        F.when(F.col("discount_rounded") >= 61, "HIGH").otherwise("LOW").alias("discount_rounded_tier"),
    )
    .agg(
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("channel") == "store", 1)), F.lit(0)), 2).alias("shipping_fee_per_store"),
        F.sum("discount").alias("total_discount"),
    )
)

result = (
    stats
    .select("store_id", "discount_rounded_tier", "store_count")
    .orderBy(F.col("store_count").desc())
)
