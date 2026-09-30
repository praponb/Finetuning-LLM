from pyspark.sql import functions as F

base = df_employees.filter(F.col("job_title") != "Director")

summary = (
    base
    .groupBy("level")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L3", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l3"),
    )
)

result = (
    summary
    .filter(F.col("pct_l3") < 46)
    .select("level", "salary_range", "pct_l3")
    .orderBy(F.col("salary_range").desc())
)
