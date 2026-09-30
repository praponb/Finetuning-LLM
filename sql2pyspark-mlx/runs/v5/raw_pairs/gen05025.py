from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(F.col("quantity").isNull() & (F.col("store_id") == 1) & F.col("quantity").between(150, 179)))
    .groupBy(
        "store_id",
        F.date_trunc("MONTH", "last_restock").alias("last_restock_month"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count("quantity").alias("quantity_count"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
    .filter(F.col("unique_stores") >= 4)
)
