from pyspark.sql import functions as F

rollup_totals = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .groupingSets(
        [["country", "city"], ["country"], []],
        F.col("p.country").alias("country"),
        F.col("p.city").alias("city"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("a.cost")).alias("min_cost"),
        F.round(F.avg(F.col("a.stock_qty")), 2).alias("avg_stock_qty"),
        F.grouping_id().alias("gid"),
    )
)

result = rollup_totals.select("min_cost")
