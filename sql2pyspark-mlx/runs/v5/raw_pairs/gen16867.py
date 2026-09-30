from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("quantity", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("order_id", "product_id")
w4 = Window.partitionBy("product_id").orderBy("unit_price", "id")
w5 = Window.orderBy("unit_price", "id")

enriched = (
    df_order_items
    .select(
        "*",
        F.first("unit_price", ignorenulls=True).over(w).alias("first_known_unit_price"),
        F.avg("unit_price").over(w2).alias("unit_price_avg_2_prior"),
        F.avg("unit_price").over(w3).alias("order_id_avg_unit_price"),
        F.row_number().over(w4).alias("row_seq"),
        F.ntile(5).over(w5).alias("unit_price_bucket"),
    )
)

result = (
    enriched
    .groupBy("product_id")
    .agg(
        F.percentile_approx("unit_price_bucket", 0.95).alias("unit_price_bucket_p95"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.bit_or("quantity").alias("quantity_bit_or_2"),
    )
    .orderBy(F.col("product_id").desc())
)
