from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .withColumn("clean_supplier_name", F.trim("supplier_name"))
    .withColumn("rating_whole", F.floor("rating"))
    .orderBy("country")
)
