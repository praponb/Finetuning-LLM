from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("salary", "id")
w3 = Window.partitionBy("job_title").orderBy(F.col("cumulative_salary").asc_nulls_first(), "id")
w4 = Window.partitionBy("dept_id").orderBy(F.col("cumulative_salary").asc_nulls_first())
w_part = Window.partitionBy("job_title", "manager_id").orderBy("cumulative_salary", "id")
w_hist = Window.partitionBy("job_title", "dept_id").orderBy(F.col("salary").desc(), "id")
w_hist_2 = Window.partitionBy("manager_id").orderBy("next_salary", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy("level")
w9 = Window.orderBy(F.col("cumulative_salary_percent_rank").desc_nulls_first())

metrics = (
    df_employees
    .filter(F.col("manager_id") == 26)
    .select(
        "*",
        F.sum("salary").over(w).alias("cumulative_salary"),
        F.lead("salary", 1, 0).over(w2).alias("next_salary"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.percent_rank().over(w4).alias("cumulative_salary_percent_rank"),
        F.lead("cumulative_salary", 2).over(w_part).alias("next_cumulative_salary"),
        F.ntile(3).over(w_hist).alias("salary_bucket"),
        F.avg("salary").over(w_hist_2).alias("salary_avg_3"),
        F.max("next_salary").over(w8).alias("level_max_next_salary"),
    )
)

remaining_keys = (
    scored
    .select("manager_id")
    .intersectAll(
        df_employees
        .filter(F.col("level") == "L2")
        .select("manager_id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select("*", F.rank().over(w9).alias("cumulative_salary_percent_rank_rank"))
)

result = (
    with_windows
    .select(
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "hire_date",
        "level",
        "cumulative_salary",
        "next_salary",
        "rank_in_group",
    )
    .orderBy("rank_in_group")
)
