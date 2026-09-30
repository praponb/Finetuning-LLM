from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("country_group", F.when(F.col("country").isin(["DE", "JP"]), "Priority").otherwise("Other"))
    .withColumn("supplier_name_prefix", F.substring("supplier_name", 1, 3))
    .orderBy(F.col("rating").desc(), "id")
    .limit(1)
)
