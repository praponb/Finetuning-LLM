from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location", "dept_name")
w3 = Window.partitionBy("dept_name")
w4 = Window.partitionBy("dept_name").orderBy("budget_share", "id")
w5 = Window.orderBy("event_seq", "id")
w_grp = Window.orderBy("seq_no_share", "id")

scored = (
    df_departments
    .filter(F.col("budget") > 3950000)
    .select(
        "id",
        "dept_name",
        "budget",
        F.row_number().over(w_ordered).alias("event_seq"),
        F.round((F.col("budget") / F.sum("budget").over(w2)), 4).alias("budget_share"),
        F.round((F.col("budget") / F.sum("budget").over(w3)), 4).alias("budget_share_2"),
    )
)

enriched = (
    scored
    .withColumn("prev_budget", F.lag("budget").over(w4))
    .withColumn("next_budget_share_2", F.lead("budget_share_2").over(w4))
    .withColumn("seq_no", F.row_number().over(w4))
)

metrics = (
    enriched
    .select(
        "id",
        "event_seq",
        "budget_share",
        "budget_share_2",
        "next_budget_share_2",
        F.lag("next_budget_share_2", 2).over(w5).alias("prev_next_budget_share_2"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w3)), 4).alias("seq_no_share"),
    )
)

scored_departments = (
    metrics
    .select(
        "*",
        F.lead("prev_next_budget_share_2").over(w_grp).alias("next_prev_next_budget_share_2"),
        F.lag("budget_share").over(w_grp).alias("prev_budget_share"),
        F.row_number().over(w_grp).alias("event_seq_2"),
    )
)

result = (
    scored_departments
    .rollup(
        F.when(F.col("next_prev_next_budget_share_2") >= 0.88, "PREMIUM").otherwise("STANDARD").alias("next_prev_next_budget_share_2_tier"),
    )
    .agg(
        F.percentile_approx("budget_share", 0.9).alias("budget_share_p90"),
        F.round(F.avg("prev_next_budget_share_2"), 2).alias("avg_prev_next_budget_share_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("prev_next_budget_share_2", [0.25, 0.5, 0.75]).alias("prev_next_budget_share_2_quartiles"),
    )
    .filter(F.col("avg_prev_next_budget_share_2") > 0)
)
