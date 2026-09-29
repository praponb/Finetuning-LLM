from pyspark.sql import functions as F

filtered_suppliers = df_suppliers.filter(F.col("supplier_name").like("Eur%") & (F.col("rating") < 2.9))

suppliers_stats = (
    filtered_suppliers
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0)).alias("singapore_count"),
        F.max("rating").alias("max_rating"),
    )
)

result = suppliers_stats.filter(F.col("max_rating") >= 3.3)
