from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("city_length", F.length("city"))
    .withColumn("supplier_name_title", F.initcap(F.trim("supplier_name")))
)
