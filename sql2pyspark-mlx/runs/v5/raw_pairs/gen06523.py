from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Chiang Mai")
    .withColumn("rating_rounded", F.round("rating", 1))
    .withColumn("supplier_name_title", F.initcap(F.trim("supplier_name")))
)
