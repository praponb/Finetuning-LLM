from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.rating").between(2.2, 2.9))
    .groupBy(F.col("s.city"))
    .agg(
        F.sum(F.when(F.col("p.brand") == "Globex", 1).otherwise(0)).alias("globex_count"),
        F.round(F.stddev(F.col("p.cost")), 2).alias("stddev_cost"),
    )
)
