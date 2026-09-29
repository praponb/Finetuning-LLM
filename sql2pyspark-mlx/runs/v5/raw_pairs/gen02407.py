from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")

lines = df_price_tiers.select("id", "plan", F.explode_outer("tier_limits").alias("element"))

with_windows = lines.select("*", F.min("element").over(w).alias("plan_min_element"))

base = (
    with_windows
    .filter(F.col("element") <= 20)
    .withColumn("plan_title", F.initcap(F.trim("plan")))
    .withColumn("plan_group", F.when(F.col("plan").isin(["starter", "basic"]), "Tier 1").otherwise("Other"))
)

valid = (
    base
    .filter(F.col("plan_min_element").between(31, 58))
    .select(
        "id",
        "element",
        "plan_min_element",
        "plan_title",
        "plan_group",
        (F.col("plan_min_element") - F.col("element")).alias("plan_min_element_minus_element"),
    )
)

result = (
    valid
    .withColumn("plan_min_element_tier", F.when(F.col("plan_min_element") >= 82, "PREMIUM").otherwise("STANDARD"))
    .withColumn("plan_min_element_minus_element_tier", F.when(F.col("plan_min_element_minus_element") >= 61, "PREMIUM").otherwise("STANDARD"))
    .rollup("plan_min_element_tier", "plan_min_element_minus_element_tier")
    .agg(
        F.percentile_approx("plan_min_element", [0.25, 0.5, 0.75]).alias("plan_min_element_quartiles"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("num_rows_2"),
        F.grouping("plan_min_element_tier").alias("grouping_level"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy(F.col("plan_min_element_minus_element_tier").desc_nulls_first())
)
