from pyspark.sql import functions as F

rollup_totals = (
    df_inventory
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .cube(F.col("d.city").alias("city"))
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("s.quantity")).alias("avg_quantity"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = rollup_totals.select("city", "rows_total", "avg_quantity", "grouping_level")
