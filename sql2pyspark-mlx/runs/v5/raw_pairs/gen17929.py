from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "channel").orderBy("line_total", "id")
w2 = Window.partitionBy("channel").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "channel").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy(F.col("discount").desc_nulls_last())
w5 = Window.orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy("amount", "id")
w7 = Window.partitionBy("order_id").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("discount_percent_rank_tier").orderBy(F.col("corr_quantity_unit_price").desc())

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
    )
)

enriched = (
    cleaned
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.quantity",
        "src.unit_price",
        "src.line_total",
        F.col("p.channel"),
        F.col("p.amount"),
        F.col("p.discount"),
    )
)

scored = (
    enriched
    .filter(F.col("unit_price").between(340, 370))
    .withColumn("prev_discount", F.lag("discount").over(w))
    .withColumn("prev_amount", F.lag("amount").over(w))
    .withColumn("next_amount", F.lead("amount").over(w))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("cumulative_line_total", F.sum("line_total").over(w2))
)

enriched_order_items = (
    scored
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "line_total",
        "amount",
        "discount",
        "prev_discount",
        "prev_amount",
        "next_amount",
        F.last("next_amount", ignorenulls=True).over(w3).alias("last_known_next_amount"),
        F.percent_rank().over(w4).alias("discount_percent_rank"),
        F.avg("discount").over(w5).alias("discount_avg_6"),
        F.lead("unit_price", 1).over(w6).alias("next_unit_price"),
        F.sum("quantity").over(w7).alias("running_quantity"),
    )
)

result = (
    enriched_order_items
    .groupBy(
        F.when(F.col("discount_percent_rank") >= 0.62, "HIGH").otherwise("LOW").alias("discount_percent_rank_tier"),
        "order_id",
    )
    .agg(
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.percentile_approx("discount_avg_6", [0.25, 0.5, 0.75]).alias("discount_avg_6_quartiles"),
    )
    .filter(F.col("corr_quantity_unit_price") > 0)
    .withColumn("corr_quantity_unit_price_percent_rank", F.percent_rank().over(w8))
    .orderBy(F.col("discount_percent_rank_tier").asc_nulls_first())
)
