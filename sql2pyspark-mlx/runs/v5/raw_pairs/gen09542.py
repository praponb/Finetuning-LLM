from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city").eqNullSafe(F.lit("Chiang Mai")))
    .withColumn("clean_supplier_name", F.trim("supplier_name"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.cost") > 300), "left_anti")
)
