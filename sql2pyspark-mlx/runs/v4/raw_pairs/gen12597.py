from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc())
w2 = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("rank_in_group").desc(), "id")
w4 = Window.partitionBy("order_id", "product_id").orderBy(F.col("row_num").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_last(), "id")

latest = (
    df_order_items
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

latest_order_items = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    latest_order_items
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.shipping_fee"), F.col("d.order_date"))
)

latest_order_items_2 = (
    enriched
    .withColumn("rank_in_group_2", F.row_number().over(w3))
    .filter(F.col("rank_in_group_2") == 1)
)

latest_order_items_3 = (
    latest_order_items_2
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

deduped = (
    latest_order_items_3
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("quarter", "order_date").alias("quarter_start"))
    .agg(
        F.percentile_approx("shipping_fee", 0.9).alias("shipping_fee_p90"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.sum("unit_price").alias("total_unit_price"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("total_unit_price") < 0)
    .orderBy(F.col("corr_quantity_unit_price").desc())
)
