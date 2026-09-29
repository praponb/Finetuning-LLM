from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(3.3, 4.0))
    .withColumn("supplier_name_prefix", F.substring("supplier_name", 1, 2))
    .withColumn("country_length", F.length("country"))
    .orderBy(F.col("supplier_name").desc())
)
