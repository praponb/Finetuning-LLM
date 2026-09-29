from pyspark.sql import functions as F

result = (
    df_order_items
    .select("id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") >= 5)
        .select("id", "product_id")
    )
)
