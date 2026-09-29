from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id", "category")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.round(F.stddev("price"), 2).alias("stddev_price"),
    )
    .filter(F.col("stddev_cost") < 31)
    .orderBy(F.col("supplier_id").desc())
)
