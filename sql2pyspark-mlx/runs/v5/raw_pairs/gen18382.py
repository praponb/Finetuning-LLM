from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc())
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("location")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

deduped = (
    df_departments
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_windows = (
    deduped
    .select(
        "id",
        "dept_name",
        "budget",
        F.count("budget").over(w2).alias("last4_count_budget"),
        F.sum("budget").over(w2).alias("last4_sum_budget"),
        F.avg("budget").over(w2).alias("last4_avg_budget"),
        F.max("budget").over(w2).alias("last4_max_budget"),
        F.round((F.col("budget") / F.sum("budget").over(w3)), 4).alias("budget_share"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.last4_count_budget",
        "s.last4_sum_budget",
        F.col("c.job_title"),
        F.col("c.salary").alias("employee_salary"),
    )
)

kept_keys = (
    with_last
    .select("id")
    .intersect(
        df_departments
        .filter(F.col("budget").between(2296000, 2721000))
        .select("id")
    )
)

reconciled = (
    with_last
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("job_title").isNotNull())
    .withColumn("last4_count_budget_tier", F.when(F.col("last4_count_budget") >= 18, "High").when(F.col("last4_count_budget") >= 6, "Medium").otherwise("Low"))
    .withColumn("last4_sum_budget_adjusted", F.round((F.col("last4_sum_budget") * 1.1), 2))
    .withColumn("job_title_clean", F.coalesce("job_title", F.lit("OTHER")))
)

result = (
    base
    .withColumn("last4_sum_budget_tier", F.when(F.col("last4_sum_budget") >= 21949000, "LARGE").otherwise("SMALL"))
    .withColumn("last4_sum_budget_adjusted_tier", F.when(F.col("last4_sum_budget_adjusted") >= 71, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["last4_sum_budget_tier", "last4_sum_budget_adjusted_tier", "job_title"], ["last4_sum_budget_tier", "last4_sum_budget_adjusted_tier"], []],
        "last4_sum_budget_tier",
        "last4_sum_budget_adjusted_tier",
        "job_title",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.sum("last4_count_budget") / F.nullif(F.count(F.when(F.col("job_title") == "Engineer", 1)), F.lit(0)), 2).alias("last4_count_budget_per_engineer"),
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_count"),
        F.grouping("last4_sum_budget_tier").alias("grouping_level"),
    )
    .filter(F.col("row_count") >= 1)
)
