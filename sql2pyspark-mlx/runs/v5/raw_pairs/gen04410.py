from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-6, -1)
w_recent = Window.partitionBy("location").orderBy("budget", "id")
w4 = Window.partitionBy("location").orderBy("prev_budget", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("location").orderBy("prev_budget")

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select("*", F.trim("location").alias("clean_location"))
)

scored = (
    filtered
    .withColumn("first_known_location", F.first("location", ignorenulls=True).over(w))
    .withColumn("budget_avg_6_prior", F.avg("budget").over(w_ordered))
)

eligible = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.year(F.col("c.hire_date")) >= 2023), "left_anti")
)

cleaned = (
    eligible
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.col("budget").cast("int").alias("budget_int"),
        F.round((F.col("budget") * 1.15), 2).alias("adjusted_budget"),
        F.coalesce("location", F.lit("NONE")).alias("location_filled"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "location",
        "budget",
        "first_known_location",
        "budget_avg_6_prior",
        "location_filled",
        F.lead("budget_avg_6_prior").over(w_recent).alias("next_budget_avg_6_prior"),
        F.lead("budget").over(w_recent).alias("next_budget"),
        F.lag("budget").over(w_recent).alias("prev_budget"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

enriched = (
    with_windows
    .filter(F.col("next_budget") >= 3168000)
    .withColumn("next_budget_avg_5", F.avg("next_budget").over(w4))
    .withColumn("prev_budget_dense_rank", F.dense_rank().over(w5))
)

result = (
    enriched
    .select(
        "id",
        "location",
        "first_known_location",
        "location_filled",
        "next_budget_avg_6_prior",
        "seq_no",
        "next_budget_avg_5",
        F.when(F.col("location").rlike("^(Chiang Mai|Berlin)$"), "CORE").when(F.col("location").rlike("^(Bangkok|New York)$"), "EXTENDED").otherwise("GROUP_A").alias("location_zone"),
    )
)
