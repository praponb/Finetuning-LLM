from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_first())

lines = df_price_tiers.select("id", "plan", F.explode_outer("tier_prices").alias("val"))

valid = (
    lines
    .filter(F.col("plan").isin(["enterprise", "pro", "basic"]))
    .withColumn("plan_upper", F.upper("plan"))
)

filtered = (
    valid
    .filter(F.col("val").isNotNull())
    .withColumn("val_filled", F.coalesce("val", F.lit(0)))
)

enriched = (
    filtered
    .filter(F.col("val") > 60)
    .select(
        "id",
        "plan",
        "plan_upper",
        "val_filled",
        F.percent_rank().over(w).alias("val_percent_rank"),
    )
)

result = (
    enriched
    .groupBy("plan")
    .agg(
        F.sum(F.when(F.col("plan") == "basic", F.col("val_percent_rank")).otherwise(0)).alias("basic_val_percent_rank"),
        F.round(F.avg("val_percent_rank"), 2).alias("avg_val_percent_rank"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.round(F.sum("val_filled") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("val_filled_per_pro"),
        F.round(F.avg("val_filled"), 2).alias("avg_val_filled"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .filter(F.col("avg_val_filled") >= 0)
)
