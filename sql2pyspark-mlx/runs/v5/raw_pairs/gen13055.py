from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("job_title").orderBy(F.col("hire_date_dayofweek").desc_nulls_first(), "id")

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-02-16").cast("date")) & F.col("dept_id").isin([18, 22]))
    .select(
        "*",
        F.when(F.col("salary") >= 134000, "High").when(F.col("salary") >= 72000, "Medium").otherwise("Low").alias("salary_level"),
        F.coalesce("level", F.lit("OTHER")).alias("resolved_level"),
    )
)

valid_employees = (
    valid
    .filter((F.col("hire_date") >= F.lit("2023-06-24").cast("date")) & F.col("level").isNotNull())
    .select(
        "id",
        "email",
        "job_title",
        "hire_date",
        "is_remote",
        "salary_level",
        F.coalesce("job_title", F.lit("unassigned")).alias("job_title_filled"),
    )
)

ranked = (
    valid_employees
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

valid_employees_2 = (
    ranked
    .filter(F.col("job_title").isNotNull())
    .select(
        "*",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.dayofweek("hire_date").alias("hire_date_dayofweek"),
        F.coalesce("job_title", F.lit("OTHER")).alias("job_title_clean"),
    )
)

deduped = (
    valid_employees_2
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "job_title", "job_title_clean")
)

result = (
    deduped
    .select(
        "id",
        "job_title",
        "job_title_clean",
        F.when(F.col("job_title").rlike("^(Analyst|Manager)$"), "GROUP_A").otherwise("EXTENDED").alias("job_title_zone"),
    )
)
