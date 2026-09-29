from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "right")
    .groupBy(
        F.when(F.col("s.country").isin(["US", "SG"]), "group_a").otherwise("other").alias("country_group"),
    )
    .agg(F.avg(F.col("p.cost")).alias("avg_cost"))
    .orderBy("country_group")
)
