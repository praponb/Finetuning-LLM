from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("product_id").orderBy("greatest_quantity_unit_price", "id")
w3 = Window.orderBy("greatest_quantity_unit_price", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("order_id", "product_id").orderBy("greatest_quantity_unit_price", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("product_id")
w_hist = Window.partitionBy("order_id").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("product_id").orderBy(F.col("event_seq").desc())
w8 = Window.partitionBy("product_id", "order_id").orderBy(F.col("greatest_quantity_unit_price_avg_6_prior").desc(), "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
    )
)

with_windows = (
    cleaned
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("prev_unit_price", F.lag("unit_price", 1).over(w2))
    .withColumn("quantity_avg_6", F.avg("quantity").over(w3))
    .withColumn("greatest_quantity_unit_price_avg_6_prior", F.avg("greatest_quantity_unit_price").over(w4))
    .withColumn("product_id_max_quantity", F.max("quantity").over(w5))
)

enriched = (
    with_windows
    .select(
        "*",
        F.last("greatest_quantity_unit_price", ignorenulls=True).over(w_hist).alias("last_known_greatest_quantity_unit_price"),
        F.percent_rank().over(w7).alias("event_seq_percent_rank"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    ranked
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .rollup(F.col("p.channel").alias("channel"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.unit_price")).alias("max_unit_price"),
        F.grouping_id().alias("grouping_level"),
    )
)
