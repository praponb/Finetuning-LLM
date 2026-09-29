from pyspark.sql import functions as F

result = (
    df_inventory
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 190, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["store_id", "quantity_tier"], ["store_id"], []],
        "store_id",
        "quantity_tier",
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity_2"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("quantity_tier").desc())
)
