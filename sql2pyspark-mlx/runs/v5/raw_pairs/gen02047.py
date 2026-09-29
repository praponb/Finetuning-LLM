from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([5, 6]))
    .groupBy(
        "product_id",
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
    )
    .agg(F.sum("quantity").alias("total_quantity"))
)
