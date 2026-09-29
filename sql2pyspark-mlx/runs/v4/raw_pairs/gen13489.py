from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([3, 32, 38]))
    .select("quantity")
    .unionByName(
        df_order_items
        .filter(F.col("order_id").isin([10, 19]))
        .select("quantity")
    )
    .orderBy("quantity")
)
