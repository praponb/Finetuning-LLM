from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("element"),
    )
)

result = (
    exploded
    .groupBy(
        "plan",
        "currency",
        F.when(F.col("element") >= 81, "HIGH").otherwise("LOW").alias("element_tier"),
    )
    .agg(
        F.avg(F.when(F.col("plan") == "enterprise", F.col("element"))).alias("enterprise_avg_element"),
        F.count_if(F.col("currency") == "EUR").alias("eur_events"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.percentile_approx("element", 0.95).alias("element_p95"),
        F.max("element").alias("max_element"),
    )
    .filter(F.col("element_p95") >= 0)
    .orderBy(F.col("currency").desc())
)
