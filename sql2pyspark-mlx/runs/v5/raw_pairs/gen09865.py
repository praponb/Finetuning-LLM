from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 186, "high").when(F.col("quantity") >= 51, "mid").otherwise("low").alias("quantity_bucket"),
        F.quarter("last_restock").alias("last_restock_quarter"),
    )
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
    .filter(F.col("quantity_range") >= 24)
    .select("quantity_bucket", "last_restock_quarter", "quantity_range")
)
