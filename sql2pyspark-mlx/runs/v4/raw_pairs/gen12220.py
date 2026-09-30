from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id")
w4 = Window.orderBy("quantity", "id").rowsBetween(-2, -1)

latest = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_dims = (
    latest
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.channel"), F.col("d.shipping_fee"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.amount"),
        F.col("p.channel").alias("order_channel"),
        F.col("p.discount"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.shipping_fee").alias("order_shipping_fee"))
)

scored = (
    joined
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w2))
    .withColumn("order_id_avg_unit_price", F.avg("unit_price").over(w3))
    .withColumn("corr_quantity_unit_price", F.corr("quantity", "unit_price").over(w3))
    .withColumn("shipping_fee_avg_2_prior", F.avg("shipping_fee").over(w4))
)

result = (
    scored
    .groupBy(
        F.when(F.col("corr_quantity_unit_price") >= 0.57, "LARGE").otherwise("SMALL").alias("corr_quantity_unit_price_tier"),
    )
    .agg(
        F.max(F.when(F.col("channel") == "partner", F.col("amount"))).alias("partner_max_amount"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count(F.when(F.col("order_channel") == "mobile", 1)).alias("mobile_count"),
        F.sum(F.when(F.col("order_channel") == "web", F.col("shipping_fee_avg_2_prior"))).alias("web_total_shipping_fee_avg_2_prior"),
    )
    .filter(F.col("partner_max_amount") < 0)
    .orderBy(F.col("web_total_shipping_fee_avg_2_prior").desc_nulls_last())
)
