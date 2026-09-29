from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("val"))
)

result = (
    lines
    .select("id", "effective_from")
    .orderBy(F.col("effective_from").asc_nulls_last(), "id")
)
