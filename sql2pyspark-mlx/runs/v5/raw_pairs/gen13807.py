from pyspark.sql import functions as F

result = (
    df_order_items
    .select("product_id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") >= 5)
        .select("product_id")
    )
)
