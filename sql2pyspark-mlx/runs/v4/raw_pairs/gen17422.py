from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.country"))
    .agg(
        F.min(F.col("p.launch_date")).alias("min_launch_date"),
        (F.max(F.col("p.cost")) - F.min(F.col("p.cost"))).alias("cost_range"),
    )
    .orderBy(F.col("min_launch_date").desc(), "country")
)
