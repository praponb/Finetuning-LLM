from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary_whole_rounded", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("level").orderBy(F.col("last3_sum_salary").desc(), "id")

valid = (
    df_employees
    .filter(F.col("email").like("%@mail.co.th") & F.col("manager_id").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.floor("salary").alias("salary_whole"),
    )
)

cleaned = (
    valid
    .filter((F.col("hire_date") >= F.lit("2023-04-21").cast("date")) & F.col("dept_id").isin([13, 34, 37]))
    .select(
        "id",
        "email",
        "job_title",
        "salary",
        "level",
        F.round("salary_whole", 1).alias("salary_whole_rounded"),
        F.coalesce("level", F.lit("N/A")).alias("level_filled"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "email",
        "salary",
        "level",
        F.sum("salary").over(w).alias("last3_sum_salary"),
        F.avg("salary").over(w).alias("last3_avg_salary"),
        F.max("salary").over(w).alias("last3_max_salary"),
    )
)

ranked = (
    scored
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "email", "last3_sum_salary", "last3_avg_salary")
)

result = (
    ranked
    .groupBy(
        F.when(F.col("last3_sum_salary") >= 628000, "PREMIUM").otherwise("STANDARD").alias("last3_sum_salary_tier"),
    )
    .agg(
        F.percentile_approx("last3_sum_salary", 0.5).alias("last3_sum_salary_p50"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("num_rows") > 1)
)
