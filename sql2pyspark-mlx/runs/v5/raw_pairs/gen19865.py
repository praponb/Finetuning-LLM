from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.col("rating") <= 2.6)
    .select("*", F.lower("city").alias("city_lower"))
)

result = (
    filtered
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.brand").isin(["Initech", "Acme", "Stark"])), "left_semi")
)
