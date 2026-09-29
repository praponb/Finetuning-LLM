from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(
        F.countDistinct("quantity").alias("unique_stores"),
        F.avg("quantity").alias("avg_quantity"),
    )
)
