from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([14, 15, 16]))
    .select("id", "order_id", "product_id")
    .unionByName(
        df_order_items
        .filter(F.col("quantity").between(3, 6))
        .select("id", "order_id", "product_id")
    )
    .orderBy("product_id")
)
