from pyspark.sql import functions as F

result = (
    df_order_items
    .select("product_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([19, 31, 35, 40]))
        .select("product_id")
    )
)
