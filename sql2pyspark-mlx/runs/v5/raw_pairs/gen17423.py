from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy("employee_level").orderBy("budget", "id")
w4 = Window.partitionBy("employee_level").orderBy(F.col("budget").desc())
w5 = Window.partitionBy("dept_name").orderBy(F.col("seq_no").desc(), "id")
w6 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("employee_level").orderBy(F.col("budget_dense_rank").desc_nulls_last())
w8 = Window.partitionBy("dept_name").orderBy(F.col("next_budget").desc(), "id")

latest_match = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location",
        "s.budget",
        F.col("c.level").alias("employee_level"),
    )
)

ranked = (
    latest_match
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    ranked
    .select(
        "id",
        "dept_name",
        "employee_level",
        F.lead("budget").over(w3).alias("next_budget"),
        F.row_number().over(w3).alias("seq_no"),
        F.dense_rank().over(w4).alias("budget_dense_rank"),
    )
)

ranked_departments = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_windows_departments = (
    ranked_departments
    .filter(F.col("seq_no").between(5, 6))
    .select(
        "*",
        F.last("dept_name", ignorenulls=True).over(w6).alias("last_known_dept_name"),
        F.cume_dist().over(w7).alias("budget_dense_rank_cume_dist"),
        F.row_number().over(w8).alias("row_seq"),
    )
)

result = (
    with_windows_departments
    .groupBy("employee_level")
    .agg(
        F.countDistinct("employee_level").alias("distinct_employee_levels"),
        F.count("*").alias("event_count"),
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("employee_level") == "L2", 1)), F.lit(0)), 2).alias("seq_no_per_l2"),
    )
    .filter(F.col("seq_no_per_l2") < 1000000)
    .orderBy("employee_level")
)
