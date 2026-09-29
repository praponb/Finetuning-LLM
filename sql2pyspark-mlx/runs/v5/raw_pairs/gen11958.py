from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_prices").alias("element"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("YEAR", "effective_from").alias("year_start"))
    .agg(
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("element_per_enterprise"),
        F.sum(F.when(F.col("plan") == "pro", F.col("element")).otherwise(0)).alias("pro_element"),
    )
    .orderBy(F.col("pro_element").desc())
)
