from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

not_flagged = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.order_id").isin([1, 2, 20, 25])), "left_semi")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.carrier"))
)

kept = (
    with_latest
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.status") == "REFUNDED"), "left_anti")
)

base = (
    kept
    .filter((F.col("order_date") >= F.lit("2023-02-20").cast("date")) & F.col("channel").isNotNull())
    .select(
        "*",
        (F.col("shipping_fee") / F.nullif("discount", F.lit(0))).alias("shipping_fee_per_discount"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_clean"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("shipping_fee_per_discount") >= 87, "LARGE").otherwise("SMALL").alias("shipping_fee_per_discount_tier"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "partner", F.col("shipping_fee")).otherwise(0)).alias("partner_shipping_fee"),
        F.sum(F.when(F.col("carrier") == "UPS", F.col("shipping_fee")).otherwise(0)).alias("ups_shipping_fee"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("shipping_fee_per_discount"), 2).alias("avg_shipping_fee_per_discount"),
    )
)
