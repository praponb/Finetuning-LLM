from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("id", "product_id", "order_id")
    .unionByName(
        df_order_items
        .filter(F.col("order_id").isin([12, 18, 31, 34]))
        .select("id", "product_id", "order_id")
    )
    .orderBy("product_id")
)
