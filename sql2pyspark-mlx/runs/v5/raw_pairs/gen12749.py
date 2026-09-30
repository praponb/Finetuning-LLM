from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "status").orderBy(F.col("greatest_quantity_unit_price").desc(), "id")
w2 = Window.partitionBy("status", "order_id").orderBy("unit_price_whole", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy("greatest_quantity_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("last7_sum_greatest_quantity_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id")
w6 = Window.partitionBy("status").orderBy(F.col("last7_sum_greatest_quantity_unit_price").desc(), "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select(
        "*",
        F.greatest("quantity", "unit_price").alias("greatest_quantity_unit_price"),
        F.ceil("unit_price").alias("unit_price_whole"),
    )
)

joined = (
    base
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.product_id",
        "s.greatest_quantity_unit_price",
        "s.unit_price_whole",
        F.col("p.status"),
    )
)

ranked = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    ranked
    .filter(F.col("unit_price_whole") == 37)
    .select(
        "*",
        F.count("greatest_quantity_unit_price").over(w2).alias("last7_count_greatest_quantity_unit_price"),
        F.sum("greatest_quantity_unit_price").over(w2).alias("last7_sum_greatest_quantity_unit_price"),
        F.avg("greatest_quantity_unit_price").over(w2).alias("last7_avg_greatest_quantity_unit_price"),
        F.max("greatest_quantity_unit_price").over(w2).alias("last7_max_greatest_quantity_unit_price"),
        F.sum("greatest_quantity_unit_price").over(w3).alias("cumulative_greatest_quantity_unit_price"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "order_id",
        "product_id",
        "greatest_quantity_unit_price",
        "unit_price_whole",
        "status",
        "last7_count_greatest_quantity_unit_price",
        "last7_avg_greatest_quantity_unit_price",
        "last7_max_greatest_quantity_unit_price",
        "cumulative_greatest_quantity_unit_price",
        F.sum("unit_price_whole").over(w4).alias("cumulative_unit_price_whole"),
        F.round((F.col("last7_count_greatest_quantity_unit_price") / F.sum("last7_count_greatest_quantity_unit_price").over(w5)), 4).alias("last7_count_greatest_quantity_unit_price_share"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

result = (
    scored
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .groupingSets(
        [["order_status", "order_item_status"], ["order_status"], []],
        F.col("o.status").alias("order_status"),
        F.col("src.status").alias("order_item_status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("src.cumulative_unit_price_whole")).alias("min_cumulative_unit_price_whole"),
        F.max(F.col("src.greatest_quantity_unit_price")).alias("max_greatest_quantity_unit_price"),
    )
    .orderBy(F.col("order_status").desc())
)
