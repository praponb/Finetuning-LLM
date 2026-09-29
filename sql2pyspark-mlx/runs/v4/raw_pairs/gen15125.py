from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.length(F.col("p.product_name")) > 12)
    .groupBy(
        F.when(F.col("s.country").isin(["TH", "US"]), "group_a").otherwise("other").alias("country_group"),
        F.col("s.country"),
    )
    .agg(F.round(F.avg(F.col("p.cost")), 2).alias("avg_cost"))
    .orderBy(F.col("country").desc(), "country_group")
    .limit(10)
)
