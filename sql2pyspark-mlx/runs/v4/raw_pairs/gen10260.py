from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").isNull() & (F.col("rating") <= 2.5))
    .withColumn("supplier_name_country", F.concat("supplier_name", F.lit(", "), "country"))
)
