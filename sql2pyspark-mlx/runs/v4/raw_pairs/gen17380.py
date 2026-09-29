from pyspark.sql import functions as F

filtered = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3]))
    .withColumn("quantity_str", F.col("quantity").cast("double"))
)

result = (
    filtered
    .select("id", "product_id")
    .exceptAll(
        df_inventory
        .filter(F.col("product_id").isin([5, 8, 9]))
        .select("id", "product_id")
    )
)
