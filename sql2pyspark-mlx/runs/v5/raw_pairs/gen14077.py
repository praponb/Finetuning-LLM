from pyspark.sql import functions as F

dim_totals = (
    df_orders
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["city", "status"], ["city"], []],
        F.col("d.city").alias("city"),
        F.col("a.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.discount")).alias("max_discount"),
        F.grouping_id().alias("level_id"),
    )
)

result = dim_totals.select("city", "status", "rows_total", "max_discount", "level_id")
