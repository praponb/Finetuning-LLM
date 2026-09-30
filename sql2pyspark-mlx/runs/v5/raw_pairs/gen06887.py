from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_first())
w3 = Window.partitionBy("dept_name").orderBy(F.col("next_budget").desc_nulls_first(), "id")
w4 = Window.partitionBy("dept_name").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_name").orderBy("event_seq", "id")
w6 = Window.partitionBy("dept_name")

base = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("location") == "London"))
    .select(
        "*",
        F.substring("location", 1, 2).alias("location_prefix"),
        F.coalesce("location", F.lit("N/A")).alias("location_filled"),
    )
)

valid = (
    base
    .filter(F.col("location") != "Singapore")
    .select(
        "id",
        "dept_name",
        "budget",
        "location_prefix",
        "location_filled",
        F.when(F.col("dept_name").isin(["Engineering", "Legal"]), "Tier 1").otherwise("Other").alias("dept_name_group"),
    )
)

with_windows = (
    valid
    .filter(F.col("dept_name") == "Marketing")
    .select(
        "*",
        F.lead("budget").over(w).alias("next_budget"),
        F.lag("budget").over(w).alias("prev_budget"),
        F.row_number().over(w).alias("event_seq"),
        F.cume_dist().over(w2).alias("budget_cume_dist"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "dept_name",
        "budget",
        "location_filled",
        "dept_name_group",
        "prev_budget",
        "event_seq",
    )
)

eligible = (
    deduped
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.job_title") == "Manager"), "left_semi")
)

kept = (
    eligible
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.dept_id").isin([3, 8, 9])), "left_anti")
)

enriched = (
    kept
    .select(
        "id",
        "dept_name",
        "budget",
        "location_filled",
        "dept_name_group",
        "prev_budget",
        F.last("prev_budget", ignorenulls=True).over(w4).alias("last_known_prev_budget"),
        F.ntile(10).over(w5).alias("event_seq_bucket"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w6)), 4).alias("event_seq_share"),
    )
)

result = (
    enriched
    .select(
        "id",
        "budget",
        "location_filled",
        "prev_budget",
        "last_known_prev_budget",
        "event_seq_bucket",
        "event_seq_share",
    )
    .orderBy(F.col("event_seq_bucket").desc())
)
