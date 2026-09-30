from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .filter(F.col("s.sqft") == 10159)
    .groupBy(
        F.when(F.year(F.col("s.opened_date")) < 2024, "before_2024").when(F.year(F.col("s.opened_date")) == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.max(F.col("i.last_restock")).alias("last_last_restock"),
        F.min(F.col("i.last_restock")).alias("min_last_restock"),
    )
    .orderBy(F.col("opened_cohort").desc(), "min_last_restock")
)
