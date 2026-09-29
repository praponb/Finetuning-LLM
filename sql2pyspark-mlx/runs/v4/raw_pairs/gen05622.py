from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") < "2023-03-01")
    .groupBy(
        F.when(F.col("quantity") >= 235, "high").when(F.col("quantity") >= 96, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") < 5)
    .select("quantity_bucket", "cnt")
    .orderBy(F.col("quantity_bucket").desc(), F.col("cnt").desc())
)
