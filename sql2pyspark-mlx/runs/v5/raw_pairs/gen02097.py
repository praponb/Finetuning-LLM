from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("shipping_fee").asc_nulls_first())
w3 = Window.partitionBy("order_id").orderBy(F.col("shipping_fee").desc_nulls_last(), "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("line_total", (F.col("unit_price") - F.col("quantity")))
    .withColumn("unit_price_int", F.col("unit_price").cast("int"))
)

enriched = (
    base
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.status") == "PENDING")
    .select(
        "s.id",
        "s.order_id",
        "s.product_id",
        "s.quantity",
        "s.unit_price",
        "s.line_total",
        "s.unit_price_int",
        F.col("d.shipping_fee"),
        F.col("d.status"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "order_id",
        "product_id",
        "shipping_fee",
        F.avg("quantity").over(w).alias("quantity_avg_2"),
        F.rank().over(w2).alias("shipping_fee_rank"),
    )
)

ranked = (
    metrics
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("id", "quantity_avg_2", "shipping_fee_rank")
)

valid = (
    ranked
    .filter(F.col("shipping_fee_rank") < 6)
    .withColumn("shipping_fee_rank_plus_quantity_avg_2", (F.col("shipping_fee_rank") + F.col("quantity_avg_2")))
    .withColumn("quantity_avg_2_filled", F.coalesce("quantity_avg_2", F.lit(0)))
)

result = (
    valid
    .groupBy(
        F.when(F.col("shipping_fee_rank_plus_quantity_avg_2") >= 66, "HIGH").otherwise("LOW").alias("shipping_fee_rank_plus_quantity_avg_2_tier"),
        F.when(F.col("quantity_avg_2") >= 9, "LARGE").otherwise("SMALL").alias("quantity_avg_2_tier"),
    )
    .agg(
        F.percentile_approx("quantity_avg_2_filled", 0.9).alias("quantity_avg_2_filled_p90"),
        F.count("*").alias("cnt"),
        F.round(F.avg("shipping_fee_rank"), 2).alias("avg_shipping_fee_rank"),
        F.max("shipping_fee_rank_plus_quantity_avg_2").alias("max_shipping_fee_rank_plus_quantity_avg_2"),
    )
)
