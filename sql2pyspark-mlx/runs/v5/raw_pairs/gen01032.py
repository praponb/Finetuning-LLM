from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary_int_minus_salary", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("dept_id").orderBy(F.col("salary_int_minus_salary").desc())
w3 = Window.partitionBy("level", "manager_id").orderBy(F.col("last4_sum_salary_int_minus_salary").desc(), "id")
w_hist = Window.partitionBy("dept_id").orderBy("salary_int_minus_salary_percent_rank", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("level").orderBy("salary_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("level").orderBy("salary_int_minus_salary_percent_rank", "id")

base = (
    df_employees
    .filter(F.col("job_title").isNotNull() & ~F.col("is_remote"))
    .select(
        "id",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.col("salary").cast("int").alias("salary_int"),
    )
)

filtered = (
    base
    .filter((F.col("hire_date") > F.lit("2023-03-28").cast("date")) & (F.col("hire_date") >= "2025-03-01"))
    .select(
        "id",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        "salary_int",
        (F.col("salary_int") - F.col("salary")).alias("salary_int_minus_salary"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.count("salary_int_minus_salary").over(w).alias("last4_count_salary_int_minus_salary"),
        F.sum("salary_int_minus_salary").over(w).alias("last4_sum_salary_int_minus_salary"),
        F.avg("salary_int_minus_salary").over(w).alias("last4_avg_salary_int_minus_salary"),
        F.percent_rank().over(w2).alias("salary_int_minus_salary_percent_rank"),
    )
)

base_employees = (
    scored
    .filter((F.col("hire_date") > F.lit("2023-02-19").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "*",
        F.ceil("salary_int_minus_salary_percent_rank").alias("salary_int_minus_salary_percent_rank_whole"),
    )
)

latest = (
    base_employees
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

scored_employees = (
    latest
    .filter(F.col("manager_id").isin([26, 29, 36]))
    .withColumn("salary_avg_3_prior", F.avg("salary").over(w_hist))
    .withColumn("cumulative_salary", F.sum("salary").over(w5))
    .withColumn("next_last4_sum_salary_int_minus_salary", F.lead("last4_sum_salary_int_minus_salary", 1, 0).over(w6))
)

result = (
    scored_employees
    .select(
        "id",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "salary_int",
        "last4_count_salary_int_minus_salary",
        "last4_sum_salary_int_minus_salary",
        "salary_int_minus_salary_percent_rank",
    )
)
