from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city") == "Singapore")
    .groupBy(F.col("s.id"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.round(F.stddev(F.col("p.cost")), 2).alias("stddev_cost"),
        F.sum(F.when(F.col("p.brand") == "Initech", F.col("p.price")).otherwise(0)).alias("initech_price"),
    )
    .filter(F.col("product_count") >= 1)
)
