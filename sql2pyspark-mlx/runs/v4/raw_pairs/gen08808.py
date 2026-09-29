from pyspark.sql import functions as F

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-05-14").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.col("quantity").cast("string").alias("quantity_str"),
    )
)

result = (
    filtered
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 227, "LARGE").otherwise("SMALL"))
    .rollup("quantity_tier")
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.count("*").alias("event_count"),
        F.bit_and("quantity").alias("quantity_bit_and_2"),
        F.count("*").alias("row_count"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("quantity_bit_and_2").desc())
)
