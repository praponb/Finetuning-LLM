from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_prices").alias("element"))
)

result = (
    lines
    .withColumn("activity_month", F.date_trunc("MONTH", "effective_from"))
    .groupingSets([["plan", "activity_month"], ["plan"]], "plan", "activity_month")
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.sum(F.when(F.col("plan") == "basic", F.col("element")).otherwise(0)).alias("basic_element"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("element"))).alias("enterprise_total_element"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
    )
)
