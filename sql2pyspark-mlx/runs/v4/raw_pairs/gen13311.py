from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(3, 5))
    .select("product_id", "order_id")
    .unionByName(
        df_order_items
        .filter(F.col("order_id").isin([5, 13, 35, 39]))
        .select("product_id", "order_id")
    )
    .distinct()
    .orderBy("product_id")
)
