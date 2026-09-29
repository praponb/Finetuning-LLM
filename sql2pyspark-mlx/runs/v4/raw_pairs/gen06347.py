from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
        "product_id",
    )
    .agg(F.countDistinct("quantity").alias("unique_stores"))
    .filter(F.col("unique_stores") > 3)
)
