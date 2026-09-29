from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select("id", "order_id", "quantity")
    .unionByName(
        df_order_items
        .filter(F.col("quantity").between(4, 6))
        .select("id", "order_id", "quantity")
    )
)
