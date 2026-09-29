from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") != "Bangkok")
    .withColumn("supplier_name_lower", F.lower("supplier_name"))
    .withColumn("country_group", F.when(F.col("country").isin(["TH", "UK"]), "Priority").otherwise("Other"))
    .orderBy("id")
)
