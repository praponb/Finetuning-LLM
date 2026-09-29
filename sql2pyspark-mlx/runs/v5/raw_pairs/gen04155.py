from pyspark.sql import functions as F

result = (
    df_order_items
    .select("id")
    .subtract(
        df_order_items
        .filter(F.col("product_id") == 8)
        .select("id")
    )
)
