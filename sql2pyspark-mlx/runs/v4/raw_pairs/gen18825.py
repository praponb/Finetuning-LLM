from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .groupBy(
        F.when(F.year(F.col("ss.sale_date")) < 2024, "before_2024").when(F.year(F.col("ss.sale_date")) == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        F.col("ss.store_id"),
    )
    .agg(F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"))
    .filter(F.col("stddev_quantity") > 21)
)
