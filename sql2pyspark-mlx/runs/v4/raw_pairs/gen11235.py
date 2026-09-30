from pyspark.sql import functions as F

result = (
    df_order_items
    .select("product_id")
    .intersect(
        df_order_items
        .filter(F.col("unit_price") >= 380)
        .select("product_id")
    )
)
