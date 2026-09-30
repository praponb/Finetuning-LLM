from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(F.col("s.country"))
    .agg(
        F.min(F.col("p.stock_qty")).alias("min_stock_qty"),
        F.round(F.avg(F.col("p.cost")), 2).alias("avg_cost"),
    )
    .orderBy("country")
    .limit(10)
)
