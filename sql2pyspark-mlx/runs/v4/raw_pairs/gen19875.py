from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.store_id") == 5)
    .groupBy(
        F.col("ss.store_id"),
        F.when(F.year(F.col("ss.sale_date")) < 2024, "before_2024").when(F.year(F.col("ss.sale_date")) == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
    )
    .orderBy("sale_cohort")
)
