from pyspark.sql import functions as F

dim_totals = (
    df_inventory
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("a.quantity")).alias("avg_quantity"),
    )
)

result = (
    dim_totals
    .select("brand", "cnt")
    .orderBy(F.col("cnt").desc())
)
