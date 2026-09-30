from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("brand").isin(["Stark", "Umbrella", "Initech"])).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.rating"), F.col("s.city"))
    .orderBy("city", F.col("rating").desc())
)
