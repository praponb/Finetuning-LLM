from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Parts%"))
    .withColumn("city_title", F.initcap(F.trim("city")))
)
