from pyspark.sql import functions as F

result = (
    df_inventory
    .withColumn("activity_month", F.date_trunc("MONTH", "last_restock"))
    .cube("activity_month", "product_id", "store_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.max("quantity").alias("max_quantity_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.avg("quantity").alias("avg_quantity"),
        F.grouping_id().alias("gid"),
    )
)
