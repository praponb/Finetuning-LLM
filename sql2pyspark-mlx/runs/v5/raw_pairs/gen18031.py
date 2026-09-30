from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("salary").desc())
w2 = Window.partitionBy("job_title")
w3 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())
w4 = Window.partitionBy("job_title", "manager_id")
w5 = Window.partitionBy("dept_id")
w6 = Window.partitionBy("job_title").orderBy(F.col("job_title_stddev_samp_salary").desc_nulls_last())
w7 = Window.orderBy(F.col("job_title_stddev_samp_salary_2").asc_nulls_first())
w8 = Window.partitionBy("dept_id").orderBy(F.col("salary_dense_rank").desc_nulls_first())
w9 = Window.orderBy(F.col("salary_dense_rank_2").desc())

filtered = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-02-13").cast("date")) & (~F.col("job_title").eqNullSafe(F.lit("Intern"))))
    .select(
        "*",
        F.date_sub("hire_date", 67).alias("hire_date_minus_67d"),
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
    )
)

matched_keys = (
    filtered
    .select("manager_id", "id")
    .subtract(
        df_employees
        .filter(F.col("hire_date").between("2024-08-01", "2025-01-01"))
        .select("manager_id", "id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .filter(F.upper(F.trim("email")) == "JACK@STARTUP.IO")
    .withColumn("salary_rank", F.rank().over(w))
    .withColumn("salary_dense_rank", F.dense_rank().over(w))
    .withColumn("job_title_stddev_samp_salary", F.stddev_samp("salary").over(w2))
    .withColumn("salary_dense_rank_2", F.dense_rank().over(w3))
    .withColumn("job_title_stddev_samp_salary_2", F.stddev_samp("salary").over(w4))
)

scored = (
    enriched
    .select(
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "hire_date",
        "email_user",
        "salary_rank",
        "salary_dense_rank",
        F.round((F.col("job_title_stddev_samp_salary") / F.sum("job_title_stddev_samp_salary").over(w5)), 4).alias("job_title_stddev_samp_salary_share"),
        F.cume_dist().over(w6).alias("job_title_stddev_samp_salary_cume_dist"),
        F.dense_rank().over(w7).alias("job_title_stddev_samp_salary_2_dense_rank"),
        F.percent_rank().over(w8).alias("salary_dense_rank_percent_rank"),
        F.rank().over(w9).alias("salary_dense_rank_2_rank"),
    )
)

enriched_employees = (
    scored
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "left")
    .select(
        "src.dept_id",
        "src.manager_id",
        "src.email_user",
        "src.salary_rank",
        "src.salary_dense_rank",
        "src.job_title_stddev_samp_salary_2_dense_rank",
        "src.salary_dense_rank_2_rank",
        F.col("p.dept_name"),
        F.col("p.location"),
        F.col("p.budget"),
    )
)

valid = (
    enriched_employees
    .filter((~F.col("location").eqNullSafe(F.lit("New York"))) & (F.col("salary_dense_rank_2_rank") != 4))
    .withColumn("salary_dense_rank_2_rank_num", F.col("salary_dense_rank_2_rank").cast("double"))
    .withColumn("location_clean", F.coalesce("location", F.lit("N/A")))
)

daily = (
    valid
    .groupBy("location", "dept_name", "dept_id")
    .agg(
        F.countDistinct("location").alias("distinct_locations"),
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("distinct_locations") >= 1)
)

result = (
    daily
    .select(
        "location",
        "dept_name",
        "dept_id",
        "distinct_locations",
        "row_count",
        "cnt",
    )
    .orderBy(F.col("dept_name").desc_nulls_last())
)
