from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").asc_nulls_first(), "id")
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w_ordered = Window.partitionBy("location").orderBy("rn", "id").rowsBetween(-2, Window.currentRow)
w_hist = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_last())

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("location_prefix", F.substring("location", 1, 4))
    .withColumn("dept_name_lower", F.lower("dept_name"))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("N/A")))
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "location", "budget", "location_prefix")
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.year(F.col("x.hire_date")) == 2023), "left_semi")
)

ranked = (
    not_flagged
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

enriched = (
    ranked
    .select(
        "*",
        F.count("budget").over(w_ordered).alias("last3_count_budget"),
        F.max("budget").over(w_ordered).alias("last3_max_budget"),
        F.min("budget").over(w_ordered).alias("last3_min_budget"),
        F.percent_rank().over(w_hist).alias("budget_percent_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "location",
        "location_prefix",
        "rn",
        "last3_count_budget",
        "last3_min_budget",
        "budget_percent_rank",
    )
)
