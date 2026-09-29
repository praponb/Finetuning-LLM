from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("hire_date_plus_months").asc_nulls_last(), "id")
w2 = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc(), "id")
w3 = Window.partitionBy("level").orderBy("rn", "id").rowsBetween(-6, -1)
w4 = Window.orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("manager_id", "dept_id").orderBy("hire_date", "id")
w6 = Window.partitionBy("job_title").orderBy(F.col("prev_adjusted_salary").asc_nulls_last(), "id")
w7 = Window.partitionBy("manager_id", "dept_id").orderBy(F.col("rn").desc())
w8 = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w9 = Window.partitionBy("dept_id").orderBy(F.col("row_seq_times_salary_avg_6_prior_p90").desc())

base = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .withColumn("adjusted_salary", F.round((F.col("salary") * 1.07), 2))
    .withColumn("hire_date_plus_months", F.add_months("hire_date", 3))
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.row_number().over(w2).alias("row_seq"),
        F.avg("salary").over(w3).alias("salary_avg_6_prior"),
        F.sum("adjusted_salary").over(w4).alias("cumulative_adjusted_salary"),
        F.lag("adjusted_salary", 1).over(w5).alias("prev_adjusted_salary"),
    )
)

deduped = (
    metrics
    .withColumn("rn_2", F.row_number().over(w6))
    .filter(F.col("rn_2") == 1)
)

latest_employees = (
    deduped
    .withColumn("row_num", F.rank().over(w7))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

base_employees = (
    latest_employees
    .filter(F.col("hire_date") >= F.lit("2023-08-21").cast("date"))
    .withColumn("adjusted_salary_avg_6_prior", F.round((F.col("salary_avg_6_prior") * 0.9), 2))
    .withColumn("row_seq_times_salary_avg_6_prior", (F.col("row_seq") * F.col("salary_avg_6_prior")))
)

latest_employees_2 = (
    base_employees
    .withColumn("latest_rank", F.row_number().over(w8))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest_employees_2
    .groupBy(
        "dept_id",
        F.when(F.col("adjusted_salary_avg_6_prior") >= 65, "PREMIUM").otherwise("STANDARD").alias("adjusted_salary_avg_6_prior_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_rows"),
        F.percentile_approx("row_seq_times_salary_avg_6_prior", 0.9).alias("row_seq_times_salary_avg_6_prior_p90"),
    )
    .filter(F.col("row_seq_times_salary_avg_6_prior_p90") > 0)
    .withColumn("row_seq_times_salary_avg_6_prior_p90_rank", F.rank().over(w9))
)
