from pyspark.sql import functions as F

dim_totals = (
    df_orders
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "inner")
    .rollup(F.col("dim.city").alias("city"), F.col("e.channel").alias("channel"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.discount")).alias("max_discount"),
    )
)

result = (
    dim_totals
    .select("channel", "max_discount")
    .orderBy(F.col("channel").asc_nulls_first())
)
