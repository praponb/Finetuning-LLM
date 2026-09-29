from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("dept_id")

matched_keys = (
    df_employees
    .select("job_title")
    .subtract(
        df_employees
        .filter(~F.col("job_title").isin(["Analyst", "Director", "Designer"]))
        .select("job_title")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .filter(F.col("level") == "L4")
    .select(
        "*",
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share"),
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share_2"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share_3"),
        F.count("salary").over(w3).alias("dept_id_count_salary"),
        F.avg("salary").over(w2).alias("level_avg_salary"),
    )
)

valid = (
    enriched
    .filter(F.col("hire_date") >= F.lit("2023-04-11").cast("date"))
    .withColumn("email_normalized", F.upper(F.trim("email")))
    .withColumn("level_filled", F.coalesce("level", F.lit("UNKNOWN")))
)

remaining_keys = (
    valid
    .select("job_title")
    .subtract(
        df_employees
        .filter(F.col("dept_id").isin([16, 29, 37, 38]))
        .select("job_title")
    )
)

kept_rows_employees = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

result = (
    kept_rows_employees
    .select("dept_id", "salary", "salary_share")
    .unpivot(
        ids=["dept_id"],
        values=["salary", "salary_share"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
