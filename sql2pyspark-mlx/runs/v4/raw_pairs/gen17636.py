from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "city").orderBy(F.col("age").desc_nulls_last(), "id")

enriched = (
    df_subscriptions
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.age"), F.col("b.city"), F.col("b.country"))
)

valid = (
    enriched
    .filter((F.col("end_date") > F.lit("2023-04-26").cast("date")) & F.col("city").isNotNull() & (F.col("age") > 46))
    .select(
        "*",
        F.initcap(F.trim("plan")).alias("plan_title"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .withColumn("monthly_fee_tier", F.when(F.col("monthly_fee") >= 240, "LARGE").otherwise("SMALL"))
    .withColumn("age_tier", F.when(F.col("age") >= 56, "HIGH").otherwise("LOW"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "end_date"))
    .rollup("monthly_fee_tier", "age_tier", "quarter_start")
    .agg(
        F.count_if(F.col("status") == "ACTIVE").alias("active_events"),
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
        F.sum(F.when(F.col("city") == "London", F.col("monthly_fee")).otherwise(0)).alias("london_monthly_fee"),
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sum(F.when(F.col("plan") == "standard", F.col("age")).otherwise(0)).alias("standard_age"),
        F.grouping("monthly_fee_tier").alias("grouping_level"),
    )
)
