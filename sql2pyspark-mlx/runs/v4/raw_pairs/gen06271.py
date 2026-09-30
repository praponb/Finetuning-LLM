from pyspark.sql import functions as F

filtered_inventory = (
    df_inventory
    .filter(F.col("product_id").isin([7, 8, 9, 10]))
    .withColumn("quantity_num", F.col("quantity").cast("double"))
)

summary = (
    filtered_inventory
    .groupBy(
        F.when(F.col("quantity_num") >= 68, "high").when(F.col("quantity_num") >= 30, "mid").otherwise("low").alias("quantity_num_bucket"),
    )
    .agg(F.countDistinct("quantity").alias("unique_products"))
)

result = (
    summary
    .select("quantity_num_bucket", "unique_products")
    .orderBy(F.col("unique_products").desc(), "quantity_num_bucket")
)
