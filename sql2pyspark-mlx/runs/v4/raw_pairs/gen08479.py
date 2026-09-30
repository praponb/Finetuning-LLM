from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

result = (
    lines
    .select("id", "effective_from", "currency", "element")
    .orderBy(F.col("effective_from").asc_nulls_last())
)
