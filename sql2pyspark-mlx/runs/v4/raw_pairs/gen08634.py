from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.sqft") >= 14595)
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.date_format(F.col("s.opened_date"), "yyyy-MM-dd").alias("opened_date_label"),
    )
    .orderBy("id", F.col("product_id").desc())
    .limit(10)
)
