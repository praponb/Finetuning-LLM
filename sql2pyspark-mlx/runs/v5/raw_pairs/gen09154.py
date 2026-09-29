from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("supplier_name").isNotNull())
    .withColumn("supplier_name_lower", F.lower("supplier_name"))
)

result = (
    valid
    .groupBy("country")
    .pivot("city", ["Bangkok", "London"])
    .agg(F.avg("rating"))
)
