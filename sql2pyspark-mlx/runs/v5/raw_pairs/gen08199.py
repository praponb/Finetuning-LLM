from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([6, 16, 19, 35]))
    .select("id", "product_id", "order_id")
    .unionByName(
        df_order_items
        .filter(F.col("unit_price").isNotNull())
        .select("id", "product_id", "order_id")
    )
    .orderBy("order_id")
)
