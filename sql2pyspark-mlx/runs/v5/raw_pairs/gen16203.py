from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.col("supplier_name").like("Pac%"))

suppliers_stats = (
    recent_suppliers
    .groupBy("country")
    .agg(F.min("rating").alias("min_rating"))
)

result = suppliers_stats.orderBy(F.col("country").desc())
