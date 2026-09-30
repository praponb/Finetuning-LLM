from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

result = (
    exploded
    .groupBy(F.date_trunc("QUARTER", "effective_from").alias("quarter_start"))
    .agg(
        F.max(F.when(F.col("plan") == "enterprise", F.col("element"))).alias("enterprise_max_element"),
        F.count_if(F.col("currency") == "THB").alias("thb_events"),
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
    )
    .filter(F.col("enterprise_max_element") >= 0)
)
