from pyspark.sql import functions as F

unpivoted = (
    df_orders
    .select(
        "id",
        "channel",
        "shipping_fee",
        F.col("amount").alias("AMOUNT"),
        F.col("discount").alias("DISCOUNT"),
    )
    .unpivot(
        ["id", "channel"],
        ["shipping_fee", "AMOUNT", "DISCOUNT"],
        "kpi",
        "metric_value",
    )
)

result = (
    unpivoted
    .groupingSets([["kpi", "channel"], ["kpi"], []], "kpi", "channel")
    .agg(
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
        F.countDistinct("kpi").alias("distinct_kpis"),
        F.max(F.when(F.col("kpi") == "DISCOUNT", F.col("metric_value"))).alias("discount_max_metric_value"),
        F.round(F.sum("metric_value") / F.nullif(F.count(F.when(F.col("kpi") == "shipping_fee", 1)), F.lit(0)), 2).alias("metric_value_per_shipping_fee"),
    )
    .orderBy("channel")
)
