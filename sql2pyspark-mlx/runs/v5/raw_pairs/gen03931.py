from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("country").isin(["DE", "TH"]))
    .select("supplier_name", "city", "rating", F.trim("city").alias("clean_city"))
    .orderBy("city", "clean_city")
)
