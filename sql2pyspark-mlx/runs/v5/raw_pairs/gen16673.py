from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("unit_price").asc_nulls_first(), "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("amount").asc_nulls_last(), "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("line_total", (F.col("quantity") * F.col("unit_price")))
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_id", "unit_price")
)

enriched = (
    deduped
    .filter(F.col("unit_price") <= 340)
    .select(
        "*",
        F.sum("unit_price").over(w2).alias("running_sum_unit_price"),
        F.count("unit_price").over(w2).alias("running_count_unit_price"),
        F.max("unit_price").over(w2).alias("running_max_unit_price"),
    )
)

enriched_order_items = (
    enriched
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.order_date"), F.col("b.amount"), F.col("b.status"))
)

deduped_order_items = (
    enriched_order_items
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped_order_items
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupingSets(
        [["order_status", "channel", "order_item_status"], ["order_status"], []],
        F.col("o.status").alias("order_status"),
        F.col("o.channel").alias("channel"),
        F.col("s.status").alias("order_item_status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.running_max_unit_price")).alias("min_running_max_unit_price"),
        F.min(F.col("s.amount")).alias("min_amount"),
        F.grouping_id().alias("gid"),
    )
)
