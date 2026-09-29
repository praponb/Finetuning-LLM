from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")
w_recent = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w4 = Window.partitionBy("dept_name").orderBy(F.col("prev_budget").desc_nulls_last(), "id")

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.coalesce("location", F.lit("unassigned")).alias("location_filled"),
        F.concat("dept_name", F.lit(" "), "location").alias("dept_name_location"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.avg("budget").over(w).alias("budget_avg_6"),
        F.lead("budget", 2).over(w2).alias("next_budget"),
        F.lag("budget", 1).over(w_recent).alias("prev_budget"),
    )
)

deduped = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .rollup(
        F.when(F.col("prev_budget") >= 3523000, "PREMIUM").otherwise("STANDARD").alias("prev_budget_tier"),
        "location",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.avg(F.when(F.col("location") == "London", F.col("budget"))).alias("london_avg_budget"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .orderBy(F.col("london_avg_budget").desc_nulls_first())
)
