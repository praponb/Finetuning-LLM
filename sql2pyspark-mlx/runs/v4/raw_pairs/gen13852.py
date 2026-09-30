from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper("supplier_name").like("%ACI%"))
    .withColumn("supplier_name_prefix", F.substring("supplier_name", 1, 2))
    .orderBy("country")
)
