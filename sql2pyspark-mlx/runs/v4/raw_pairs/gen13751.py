from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Glo%") & (F.col("country") == "US"))
    .withColumn("country_upper", F.upper("country"))
    .orderBy(F.col("country").desc())
)
