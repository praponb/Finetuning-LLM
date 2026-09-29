from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("distinct_plans").desc())

flattened = df_price_tiers.select("id", "plan", F.explode_outer("tier_prices").alias("element"))

result = (
    flattened
    .groupBy(
        "plan",
        F.when(F.col("element") >= 70, "LARGE").otherwise("SMALL").alias("element_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.percentile_approx("element", 0.5).alias("element_p50"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.countDistinct("plan").alias("distinct_plans_2"),
        F.sum(F.when(F.col("plan") == "pro", F.col("element")).otherwise(0)).alias("pro_element"),
    )
    .filter(F.col("element_p50") < 0)
    .withColumn("distinct_plans_rank", F.rank().over(w))
    .orderBy(F.col("plan").asc_nulls_first())
)
