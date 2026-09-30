from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(~(F.col("store_id").isin([1, 3]) & (F.col("last_restock") >= "2024-10-07")))
)

summary = (
    valid_inventory
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = summary.orderBy("last_restock_cohort")
