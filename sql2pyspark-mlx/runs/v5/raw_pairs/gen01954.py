from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(-6, Window.currentRow)
w_part = Window.orderBy("budget_avg_6", "id")
w4 = Window.orderBy(F.col("budget_avg_6").desc(), "id")
w5 = Window.orderBy(F.col("next_budget_avg_6").desc(), "id")
w6 = Window.orderBy("prev_budget_avg_6", "id").rowsBetween(-5, -1)
w7 = Window.orderBy("budget_avg_6", "id").rowsBetween(-3, -1)

filtered = (
    df_departments
    .filter(F.col("location").isNotNull() & (~F.col("dept_name").eqNullSafe(F.lit("Marketing"))))
    .select(
        "*",
        F.when(F.col("location").isin(["Chiang Mai", "Singapore"]), "Core").otherwise("Other").alias("location_group"),
        F.lower("location").alias("location_lower"),
        F.coalesce("location", F.lit("N/A")).alias("location_filled"),
    )
)

enriched = (
    filtered
    .filter(F.col("location").isin(["New York", "Tokyo"]))
    .select(
        "id",
        "location_group",
        "location_filled",
        F.lag("budget").over(w_ordered).alias("prev_budget"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.avg("budget").over(w2).alias("budget_avg_6"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "location_group",
        "prev_budget",
        "budget_avg_6",
        F.lag("event_seq").over(w_part).alias("prev_event_seq"),
        F.lead("budget_avg_6").over(w_part).alias("next_budget_avg_6"),
        F.lag("budget_avg_6").over(w_part).alias("prev_budget_avg_6"),
        F.row_number().over(w_part).alias("event_seq_2"),
    )
)

with_windows_departments = (
    with_windows
    .select(
        "*",
        F.row_number().over(w4).alias("event_seq"),
        F.ntile(10).over(w5).alias("next_budget_avg_6_bucket"),
        F.avg("event_seq_2").over(w6).alias("event_seq_2_avg_5_prior"),
        F.avg("prev_budget").over(w7).alias("prev_budget_avg_3_prior"),
    )
)

result = (
    with_windows_departments
    .select(
        "id",
        "location_group",
        "prev_budget",
        "budget_avg_6",
        "prev_event_seq",
        "next_budget_avg_6",
        "event_seq",
        "event_seq_2_avg_5_prior",
    )
    .orderBy(F.col("prev_budget").desc_nulls_last())
)
