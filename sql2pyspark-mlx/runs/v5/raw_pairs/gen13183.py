from pyspark.sql import functions as F

result = (
    df_inventory
    .withColumn("quarter_start", F.date_trunc("QUARTER", "last_restock"))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 225, "HIGH").otherwise("LOW"))
    .rollup("quarter_start", "quantity_tier")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("distinct_product_ids") > 1)
    .orderBy("distinct_store_ids")
)
