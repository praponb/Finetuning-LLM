from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .withColumn("is_singapore", F.when(F.col("city") == "Singapore", 1).otherwise(0))
    .withColumn("supplier_name_prefix", F.substring("supplier_name", 1, 4))
)
