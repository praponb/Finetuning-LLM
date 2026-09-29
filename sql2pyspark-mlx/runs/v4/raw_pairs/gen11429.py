from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select("id", "created_at", "coupon_codes", F.col("line.qty"), F.col("line.promo"))
)

result = (
    lines
    .select("promo")
    .orderBy(F.col("promo").desc_nulls_last())
)
