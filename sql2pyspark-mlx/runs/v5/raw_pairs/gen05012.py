from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("element"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("YEAR", "effective_from").alias("year_start"), "plan")
    .agg(
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_count"),
        F.sum(F.when(F.col("plan") == "basic", F.col("element")).otherwise(0)).alias("basic_element"),
        F.count("*").alias("num_rows"),
    )
)
