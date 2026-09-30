from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 238, "high").when(F.col("quantity") >= 76, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.sum("quantity").alias("total_quantity"))
    .filter(F.col("total_quantity") < 970)
    .select("quantity_bucket", "total_quantity")
    .orderBy("quantity_bucket", F.col("total_quantity").desc())
)
