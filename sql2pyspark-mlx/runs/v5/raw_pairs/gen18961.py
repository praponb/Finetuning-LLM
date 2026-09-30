from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") <= 2.8) & F.col("supplier_name").like("%Par%") & F.col("city").isNull())
    .select("city", "rating", F.col("rating").cast("int").alias("rating_int"))
    .orderBy(F.col("rating").desc())
)
