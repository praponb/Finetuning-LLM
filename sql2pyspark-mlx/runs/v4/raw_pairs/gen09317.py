from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(
        F.col("s.country"),
        F.when(F.col("s.city").isin(["New York", "Bangkok"]), "group_a").otherwise("other").alias("city_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.brand") == "Umbrella", 1).otherwise(0))) / F.count("*")), 1).alias("pct_umbrella"),
        F.max(F.col("p.launch_date")).alias("last_launch_date"),
    )
    .orderBy(F.col("city_group").desc())
)
