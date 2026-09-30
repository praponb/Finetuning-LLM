from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first())
w2 = Window.partitionBy("dept_name")
w3 = Window.partitionBy("dept_name").orderBy("budget", "id")
w4 = Window.partitionBy("dept_name").orderBy("budget_rounded", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("budget", "id")
w7 = Window.orderBy(F.col("budget_bucket").desc(), "id")
w8 = Window.partitionBy("dept_name").orderBy("dept_name_stddev_samp_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("dept_name").orderBy("next_budget_rounded", "id")
w10 = Window.orderBy(F.col("event_seq").desc_nulls_last())
w11 = Window.orderBy(F.col("budget_rounded_avg_2_prior").asc_nulls_first())

cleaned = (
    df_departments
    .filter(F.col("location") == "Singapore")
    .withColumn("budget_rounded", F.round("budget", 2))
)

deduped = (
    cleaned
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "dept_name", "budget", "budget_rounded")
)

metrics = (
    deduped
    .select(
        "*",
        F.stddev_samp("budget").over(w2).alias("dept_name_stddev_samp_budget"),
        F.ntile(10).over(w3).alias("budget_bucket"),
        F.avg("budget_rounded").over(w4).alias("budget_rounded_avg_2_prior"),
        F.last("dept_name", ignorenulls=True).over(w5).alias("last_known_dept_name"),
        F.lead("budget_rounded", 2).over(w6).alias("next_budget_rounded"),
    )
)

metrics_departments = (
    metrics
    .select(
        "*",
        F.row_number().over(w7).alias("event_seq"),
        F.last("dept_name", ignorenulls=True).over(w8).alias("last_known_dept_name_2"),
        F.lead("budget_bucket", 1).over(w9).alias("next_budget_bucket"),
    )
)

valid = (
    metrics_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.when(F.col("budget_rounded_avg_2_prior") >= 69, "High").when(F.col("budget_rounded_avg_2_prior") >= 19, "Medium").otherwise("Low").alias("budget_rounded_avg_2_prior_band"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.cume_dist().over(w10).alias("event_seq_cume_dist"),
        F.rank().over(w11).alias("budget_rounded_avg_2_prior_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "budget",
        "budget_rounded",
        "budget_bucket",
        "budget_rounded_avg_2_prior",
        "last_known_dept_name",
        "event_seq",
        "next_budget_bucket",
        "budget_rounded_avg_2_prior_band",
    )
)
