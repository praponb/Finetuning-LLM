from pyspark.sql import functions as F

dim_totals = (
    df_inventory
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .cube(F.col("p.city").alias("city"))
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("a.quantity")).alias("avg_quantity"),
    )
)

result = (
    dim_totals
    .select("city", "rows_total", "avg_quantity")
    .orderBy(F.col("avg_quantity").desc_nulls_first())
)
