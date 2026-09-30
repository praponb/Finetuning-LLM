from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("level").orderBy("latest_rank", "id")
w4 = Window.partitionBy("job_title", "manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("level", "manager_id").orderBy("salary", "id")
w6 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("manager_id")
w8 = Window.partitionBy("level").orderBy("salary_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("job_title", "level").orderBy(F.col("salary").desc())

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-10-09").cast("date")) & F.col("manager_id").isNotNull() & (F.col("salary") >= 108000))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "salary",
        "hire_date",
        F.substring("email", 1, 3).alias("email_prefix"),
    )
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

joined = (
    deduped
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id") == F.col("d.id"), "left")
    .select(
        "src.*",
        F.col("d.level"),
        F.col("d.job_title"),
        F.col("d.email").alias("employee_email"),
    )
)

ranked = (
    joined
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.lead("salary", 1).over(w3).alias("next_salary"),
        F.last("level", ignorenulls=True).over(w4).alias("last_known_level"),
        F.ntile(5).over(w5).alias("salary_bucket"),
        F.last("level", ignorenulls=True).over(w6).alias("last_known_level_2"),
        F.round((F.col("salary") / F.sum("salary").over(w7)), 4).alias("salary_share"),
    )
)

scored_employees = (
    scored
    .withColumn("running_salary", F.sum("salary").over(w8))
    .withColumn("salary_percent_rank", F.percent_rank().over(w9))
)

result = (
    scored_employees
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "salary",
        "hire_date",
        "email_prefix",
        "employee_email",
        "latest_rank",
    )
    .orderBy(F.col("id").asc_nulls_last(), "salary")
    .limit(5)
)
