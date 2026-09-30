from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan")
w_hist = Window.partitionBy("plan").orderBy(F.col("val").desc())
w_grp = Window.orderBy(F.col("val").desc_nulls_first())

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_prices").alias("val"))
)

metrics = (
    exploded
    .withColumn("plan_min_val", F.min("val").over(w_part))
    .withColumn("plan_max_val", F.max("val").over(w_part))
)

scored = (
    metrics
    .select(
        "*",
        F.cume_dist().over(w_hist).alias("val_cume_dist"),
        F.count("plan_max_val").over(w_part).alias("plan_count_plan_max_val"),
        F.cume_dist().over(w_grp).alias("val_cume_dist_2"),
    )
)

grouped = (
    scored
    .groupBy(F.when(F.col("val") >= 74, "LARGE").otherwise("SMALL").alias("val_tier"))
    .agg(
        F.avg(F.when(F.col("plan") == "enterprise", F.col("plan_count_plan_max_val"))).alias("enterprise_avg_plan_count_plan_max_val"),
        F.round(F.sum("val_cume_dist_2") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("val_cume_dist_2_per_enterprise"),
    )
)

result = (
    grouped
    .select("val_tier", "enterprise_avg_plan_count_plan_max_val")
    .orderBy("enterprise_avg_plan_count_plan_max_val", "val_tier")
)
