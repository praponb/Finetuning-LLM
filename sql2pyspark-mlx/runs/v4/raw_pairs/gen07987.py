from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("element"))
)

result = (
    flattened
    .groupBy(
        F.when(F.col("element") >= 69, "PREMIUM").otherwise("STANDARD").alias("element_tier"),
    )
    .agg(
        F.max(F.when(F.col("plan") == "basic", F.col("element"))).alias("basic_max_element"),
        F.count_if(F.col("plan") == "pro").alias("pro_events"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sort_array(F.collect_set("plan")).alias("plan_list_2"),
        F.max(F.when(F.col("plan") == "starter", F.col("element"))).alias("starter_max_element"),
        F.round(F.avg("element"), 2).alias("avg_element"),
    )
    .orderBy("pro_events")
)
