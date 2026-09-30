from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("id", "product_id", "quantity")
    .unionByName(
        df_order_items
        .filter(F.col("product_id").isin([2, 9, 34, 35]))
        .select("id", "product_id", "quantity")
    )
    .distinct()
)
