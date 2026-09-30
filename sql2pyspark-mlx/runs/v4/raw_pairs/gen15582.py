from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.upper("supplier_name").like("%OKY%") | F.col("supplier_name").like("Lio%")))
    .withColumn("is_de", F.when(F.col("country") == "DE", 1).otherwise(0))
    .withColumn("rating_tier", F.when(F.col("rating") >= 3.8, "High").when(F.col("rating") >= 2.0, "Medium").otherwise("Low"))
    .orderBy("supplier_name", "id")
    .limit(20)
)
