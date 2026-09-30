from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([2, 14, 32, 37]))
    .select("order_id")
    .unionByName(
        df_order_items
        .filter(F.col("order_id").isin([15, 25, 28]))
        .select("order_id")
    )
)
