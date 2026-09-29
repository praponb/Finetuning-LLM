from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 2.5) & (F.length("supplier_name") > 11))
    .select("id", "city", F.length("city").alias("city_length"))
)
