from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", "plan", F.explode("discounts").alias("val"))

result = (
    exploded
    .groupBy("plan")
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
    .filter(F.col("distinct_plans") > 1)
    .orderBy(F.col("plan").desc_nulls_last())
)
