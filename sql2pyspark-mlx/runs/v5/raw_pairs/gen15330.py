from pyspark.sql import functions as F

exploded = (
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
    exploded
    .groupBy(F.date_trunc("month", "effective_from").alias("activity_month"))
    .agg(
        F.avg(F.when(F.col("plan") == "starter", F.col("element"))).alias("starter_avg_element"),
        F.count("*").alias("cnt"),
        F.percentile_approx("element", 0.5).alias("element_p50"),
    )
    .filter(F.col("element_p50") > 0)
    .orderBy(F.col("starter_avg_element").desc())
)
