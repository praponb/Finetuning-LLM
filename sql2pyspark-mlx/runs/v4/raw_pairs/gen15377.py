from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([7, 8, 15, 26]))
    .withColumn("quantity_whole", F.floor("quantity"))
    .withColumn("quantity_str", F.col("quantity").cast("double"))
)

result = (
    base
    .select("product_id")
    .exceptAll(
        df_order_items
        .filter(F.col("quantity").between(7, 8))
        .select("product_id")
    )
)
