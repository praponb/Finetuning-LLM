from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.opened_date").between("2023-12-01", "2024-08-01"))
    .groupBy(
        F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"),
        F.col("s.region"),
    )
    .agg(
        F.max(F.col("i.quantity")).alias("max_quantity"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("max_quantity") > 60)
    .orderBy("region")
)
