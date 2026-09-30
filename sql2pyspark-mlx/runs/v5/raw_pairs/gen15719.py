from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.product_id").isin([3, 4]))
    .groupBy(
        F.col("ss.store_id"),
        F.when(F.year(F.col("ss.sale_date")) < 2024, "before_2024").when(F.year(F.col("ss.sale_date")) == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
    )
    .orderBy("latest_last_restock", F.col("store_id").desc(), "sale_cohort")
    .limit(5)
)
