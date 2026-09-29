from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Berlin"))))
    .withColumn("supplier_name_prefix", F.substring("supplier_name", 1, 3))
)

result = (
    base
    .groupBy("city")
    .pivot("country", ["SG", "TH"])
    .agg(F.avg("rating"))
)
