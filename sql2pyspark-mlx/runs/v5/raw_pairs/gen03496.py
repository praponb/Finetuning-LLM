from pyspark.sql import functions as F

base = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-03-15"))) & (~F.col("job_title").eqNullSafe(F.lit("Designer"))))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.round("salary", 2).alias("salary_rounded"),
        F.round((F.col("salary") * 100), 2).alias("salary_adjusted"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_clean"),
    )
)

result = (
    base
    .groupBy("manager_id")
    .pivot("job_title", ["Engineer", "Analyst", "Designer"])
    .agg(F.avg("salary").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "manager_id",
        F.col("Engineer_total").alias("engineer_total"),
        F.col("Engineer_cnt").alias("engineer_cnt"),
        F.col("Analyst_total").alias("analyst_total"),
        F.col("Analyst_cnt").alias("analyst_cnt"),
        F.col("Designer_total").alias("designer_total"),
        F.col("Designer_cnt").alias("designer_cnt"),
    )
)
