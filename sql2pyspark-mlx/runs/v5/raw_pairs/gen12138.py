from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~((F.col("rating") > 2.4) & F.col("city").isin(["Berlin", "Chiang Mai"])))
    .select("city", "supplier_name", "rating", F.trim("country").alias("clean_country"))
)
