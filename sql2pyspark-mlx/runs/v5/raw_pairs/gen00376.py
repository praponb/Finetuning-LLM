from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "SG") & (F.length("supplier_name") > 6))
    .withColumn("rating_rounded", F.round("rating", 0))
    .orderBy(F.col("id").desc())
)
