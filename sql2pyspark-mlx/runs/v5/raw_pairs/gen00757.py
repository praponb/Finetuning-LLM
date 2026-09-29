from pyspark.sql import functions as F

result = (
    df_orders
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .cube(F.col("p.city").alias("city"), F.col("e.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("e.discount")).alias("total_discount"),
        F.min(F.col("e.amount")).alias("min_amount"),
    )
)
