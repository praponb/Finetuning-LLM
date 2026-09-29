from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc())
w_recent = Window.partitionBy("product_id", "order_id")
w3 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc())
w4 = Window.partitionBy("product_id")
w5 = Window.partitionBy("order_id")
revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.max("revenue").alias("max_store_sale_revenue"))

with_windows = (
    df_order_items
    .withColumn("quantity_cume_dist", F.cume_dist().over(w))
    .withColumn("product_id_count_unit_price", F.count("unit_price").over(w_recent))
    .withColumn("unit_price_rank", F.rank().over(w3))
    .withColumn("product_id_sum_quantity", F.sum("quantity").over(w4))
    .withColumn("unit_price_share", F.round((F.col("unit_price") / F.sum("unit_price").over(w5)), 4))
)

result = (
    with_windows
    .join(revenue_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "unit_price",
        "quantity_cume_dist",
        "product_id_count_unit_price",
        "unit_price_rank",
        "product_id_sum_quantity",
        F.col("max_store_sale_revenue"),
    )
    .orderBy(F.col("max_store_sale_revenue").asc_nulls_last())
)
