from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("dept_name").orderBy(F.col("is_singapore").desc())
w3 = Window.partitionBy("location").orderBy("is_singapore", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("budget").isNotNull())
    .select(
        "*",
        F.when(F.col("location") == "Singapore", 1).otherwise(0).alias("is_singapore"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.sum("is_singapore").over(w).alias("running_is_singapore"),
        F.dense_rank().over(w_grp).alias("is_singapore_dense_rank"),
        F.lag("budget", 2).over(w3).alias("prev_budget"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location",
        "s.is_singapore",
        "s.is_singapore_dense_rank",
        F.col("c.job_title"),
        F.col("c.salary"),
    )
)

kept_keys = (
    with_latest
    .select("id", "dept_name")
    .subtract(
        df_departments
        .filter(F.col("location") == "Berlin")
        .select("id", "dept_name")
    )
)

verified = (
    with_latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("salary_tier", F.when(F.col("salary") >= 150000, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["salary_tier", "location"], ["salary_tier"], []],
        "salary_tier",
        "location",
    )
    .agg(
        F.round(F.sum("is_singapore_dense_rank") / F.nullif(F.count(F.when(F.col("location") == "Singapore", 1)), F.lit(0)), 2).alias("is_singapore_dense_rank_per_singapore"),
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_rows"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_rows"),
        F.grouping("salary_tier").alias("grouping_id"),
    )
    .filter(F.col("is_singapore_dense_rank_per_singapore") > 0)
    .orderBy(F.col("salary_tier").desc_nulls_last())
)
