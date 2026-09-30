from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([9, 26]))
    .select("quantity")
    .intersect(
        df_order_items
        .filter(F.col("order_id").isin([5, 17, 33, 40]))
        .select("quantity")
    )
)
