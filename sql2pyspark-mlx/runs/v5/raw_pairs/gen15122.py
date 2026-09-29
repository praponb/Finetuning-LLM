from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("level").orderBy("is_remote_flag", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("level").orderBy("is_remote_flag", "id")
w_part = Window.orderBy("last4_min_salary", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("level")
w5 = Window.partitionBy("job_title").orderBy("last4_max_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-09-04").cast("date")) & (F.col("salary") < 132000))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("level") != "L2")
    .select(
        "*",
        F.max("salary").over(w_hist).alias("last4_max_salary"),
        F.min("salary").over(w_hist).alias("last4_min_salary"),
        F.avg("salary").over(w_hist).alias("last4_avg_salary"),
        F.count("salary").over(w_hist).alias("last4_count_salary"),
    )
)

scored = (
    with_windows
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("last4_avg_salary_avg_3", F.avg("last4_avg_salary").over(w_part))
)

enriched = (
    scored
    .select(
        "id",
        "full_name",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "last4_avg_salary",
        "last4_count_salary",
        "seq_no",
        "last4_avg_salary_avg_3",
        F.round((F.col("last4_avg_salary") / F.sum("last4_avg_salary").over(w4)), 4).alias("last4_avg_salary_share"),
        F.first("level", ignorenulls=True).over(w5).alias("first_known_level"),
    )
)

result = (
    enriched
    .groupBy("seq_no")
    .agg(
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_rows"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("job_title") == "Manager", 1)), F.lit(0)), 2).alias("salary_per_manager"),
    )
    .orderBy(F.col("engineer_rows").asc_nulls_last())
)
