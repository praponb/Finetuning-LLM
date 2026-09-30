from pyspark.sql import functions as F

result = (
    df_order_items
    .select("product_id", "id")
    .intersectAll(
        df_order_items
        .filter(F.col("unit_price") < 260)
        .select("product_id", "id")
    )
)
