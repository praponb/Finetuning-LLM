from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .filter(F.col("s.rating") > 3.1)
    .groupBy(
        F.col("s.country"),
        F.when(F.col("s.country").isin(["SG", "US"]), "priority").otherwise("other").alias("country_group"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev(F.col("p.cost")), 2).alias("stddev_cost"),
    )
)
