from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc(), "id")
w_hist = Window.partitionBy("manager_id", "job_title").orderBy("hire_date_plus_months", "id")
w3 = Window.partitionBy("dept_id", "job_title").orderBy("hire_date_plus_months", "id")
w4 = Window.orderBy("next_salary", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy("next_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("next_salary", "id")

deduped = (
    df_employees
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

valid = (
    deduped
    .filter(F.col("hire_date") >= F.lit("2023-06-19").cast("date"))
    .select(
        "*",
        F.add_months("hire_date", 3).alias("hire_date_plus_months"),
        F.coalesce("level", F.lit("N/A")).alias("resolved_level"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "hire_date",
        "hire_date_plus_months",
        "resolved_level",
        F.lead("salary").over(w_hist).alias("next_salary"),
        F.lag("salary").over(w_hist).alias("prev_salary"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "hire_date_plus_months",
        "resolved_level",
        "next_salary",
        "prev_salary",
        "seq_no",
    )
)

enriched_employees = (
    latest
    .withColumn("prev_salary_avg_2", F.avg("prev_salary").over(w4))
    .withColumn("running_seq_no", F.sum("seq_no").over(w5))
    .withColumn("next_salary_bucket", F.ntile(4).over(w6))
)

result = (
    enriched_employees
    .select(
        "id",
        "hire_date_plus_months",
        "next_salary",
        "prev_salary",
        "prev_salary_avg_2",
    )
    .orderBy(F.col("hire_date_plus_months").asc_nulls_first())
)
