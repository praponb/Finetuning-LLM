from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("unit_price").desc(), "id")
w2 = Window.orderBy("unit_price_rounded", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")
w4 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id", "order_id").orderBy(F.col("cumulative_unit_price_rounded").desc_nulls_first(), "id")
w6 = Window.partitionBy("order_id")
w7 = Window.partitionBy("order_id").orderBy("unit_price_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("order_id", "channel").orderBy("next_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("unit_price_rounded", F.round("unit_price", 0))
)

scored = (
    base
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("seq_no"),
        F.lead("quantity", 1, 0).over(w2).alias("next_quantity"),
        F.row_number().over(w3).alias("row_seq"),
        F.sum("unit_price_rounded").over(w4).alias("cumulative_unit_price_rounded"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    latest
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.channel"), F.col("d.discount"), F.col("d.amount"))
)

metrics = (
    enriched
    .withColumn("order_id_avg_amount", F.avg("amount").over(w6))
    .withColumn("first_known_unit_price_rounded", F.first("unit_price_rounded", ignorenulls=True).over(w7))
    .withColumn("running_event_seq", F.sum("event_seq").over(w8))
)

with_dims = (
    metrics
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select(
        "src.*",
        F.col("d.order_date"),
        F.col("d.discount").alias("order_discount"),
        F.col("d.status"),
    )
)

with_dims_order_items = (
    with_dims
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select(
        "src.*",
        F.col("b.order_date").alias("order_order_date"),
        F.col("b.discount").alias("order_discount_2"),
    )
)

result = (
    with_dims_order_items
    .groupBy(F.date_trunc("YEAR", "order_date").alias("year_start"))
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
