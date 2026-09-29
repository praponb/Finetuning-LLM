from pyspark.sql import functions as F

base = (
    df_inventory
    .filter((F.col("last_restock") <= "2023-05-18") & F.col("product_id").isin([2, 5, 6]) & (F.col("last_restock") > "2024-04-23"))
)

agg = (
    base
    .groupBy(F.month("last_restock").alias("last_restock_month"))
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    agg
    .filter(F.col("total_quantity") >= 900)
    .select("last_restock_month", "total_quantity")
    .orderBy(F.col("total_quantity").desc(), F.col("last_restock_month").desc())
)
