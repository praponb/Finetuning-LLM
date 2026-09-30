from pyspark.sql import functions as F

result = (
    df_order_items
    .select("order_id", "product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("quantity") >= 8)
        .select("order_id", "product_id")
    )
)
