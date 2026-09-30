from pyspark.sql import functions as F

result = (
    df_orders
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["city", "status"], ["city"]],
        F.col("p.city").alias("city"),
        F.col("a.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("a.discount")).alias("avg_discount"),
        F.min(F.col("a.amount")).alias("min_amount"),
    )
)
