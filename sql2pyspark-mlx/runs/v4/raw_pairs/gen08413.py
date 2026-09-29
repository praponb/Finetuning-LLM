from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.quarter("last_restock").alias("last_restock_quarter"),
        F.when(F.col("quantity") >= 194, "high").when(F.col("quantity") >= 31, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 5)
    .select("last_restock_quarter", "quantity_bucket", "max_quantity", "cnt")
)
