from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").asc_nulls_first())
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity").asc_nulls_last())
w3 = Window.orderBy("quantity", "id")
w4 = Window.partitionBy("order_id", "product_id").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id", "product_id").orderBy("latest_rank", "id")
w6 = Window.partitionBy("product_id")
w7 = Window.partitionBy("product_id").orderBy(F.col("corr_unit_price_quantity").desc(), "id")

deduped = (
    df_order_items
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

ranked = (
    deduped
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    ranked
    .select(
        "*",
        F.lag("unit_price", 1).over(w3).alias("prev_unit_price"),
        F.sum("quantity").over(w4).alias("cumulative_quantity"),
        F.rank().over(w2).alias("quantity_rank"),
        F.lead("quantity", 1, 0).over(w5).alias("next_quantity"),
        F.corr("unit_price", "quantity").over(w6).alias("corr_unit_price_quantity"),
    )
)

ranked_order_items = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

summary = (
    ranked_order_items
    .groupBy(
        F.when(F.col("quantity_rank") >= 8, "HIGH").otherwise("LOW").alias("quantity_rank_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("cumulative_quantity"), 2).alias("avg_cumulative_quantity"),
        F.bit_and("quantity").alias("quantity_bit_and"),
    )
    .filter(F.col("event_count") < 1)
)

result = (
    summary
    .select("quantity_rank_tier", "avg_cumulative_quantity")
    .orderBy("quantity_rank_tier")
)
