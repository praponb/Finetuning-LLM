from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(5, 6))
    .select("quantity", "product_id")
    .unionByName(
        df_order_items
        .filter(F.col("unit_price") <= 240)
        .select("quantity", "product_id")
    )
    .distinct()
    .orderBy(F.col("quantity").desc())
)
