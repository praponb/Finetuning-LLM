from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .filter(F.col("s.city") != "Chiang Mai")
    .groupBy(F.col("s.city"))
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") > 4)
)
