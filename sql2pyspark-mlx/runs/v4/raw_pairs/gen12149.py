from pyspark.sql import functions as F

lines = df_price_tiers.select("id", F.explode("discounts").alias("element"))

result = (
    lines
    .select("id", "element")
    .orderBy(F.col("element").asc_nulls_first(), "id")
)
