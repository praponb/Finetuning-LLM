from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") > 4.1)
    .withColumn("clean_country", F.trim("country"))
)
