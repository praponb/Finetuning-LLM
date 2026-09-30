from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .groupBy(F.col("s.city"))
    .agg(
        F.sum(F.when(F.col("p.brand") == "Umbrella", F.col("p.price")).otherwise(0)).alias("umbrella_price"),
    )
    .filter(F.col("umbrella_price") >= 76)
    .orderBy(F.col("city").desc())
)
