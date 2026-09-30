from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([14, 18, 28]))
    .select("id", "order_id")
    .unionByName(
        df_order_items
        .filter(F.col("product_id").isNull())
        .select("id", "order_id")
    )
    .distinct()
)
