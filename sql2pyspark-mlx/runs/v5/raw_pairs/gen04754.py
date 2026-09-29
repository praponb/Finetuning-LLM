from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "plan", F.explode("discounts").alias("element"))

result = (
    lines
    .groupBy(
        F.when(F.col("element") >= 89, "LARGE").otherwise("SMALL").alias("element_tier"),
        "plan",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("element", 0.5).alias("element_p50"),
        F.sum(F.when(F.col("plan") == "starter", F.col("element")).otherwise(0)).alias("starter_element"),
    )
    .filter(F.col("starter_element") < 1000000)
)
