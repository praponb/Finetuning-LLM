from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(95, 173))
    .groupBy(
        F.when(F.col("quantity") >= 186, "high").when(F.col("quantity") >= 60, "mid").otherwise("low").alias("quantity_bucket"),
        "store_id",
    )
    .agg(F.avg("quantity").alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 170)
    .select("quantity_bucket", "store_id", "avg_quantity")
    .orderBy("quantity_bucket")
)
