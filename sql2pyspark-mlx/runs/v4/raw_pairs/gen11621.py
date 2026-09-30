from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("L%"))
    .groupBy("country")
    .agg(F.count("*").alias("supplier_count"))
    .orderBy(F.col("country").desc(), "supplier_count")
)
