from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Manager", "Designer", "Intern"]) & (F.col("level") == "L5"))
    .groupBy("job_title")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0))) / F.count("*")), 1).alias("pct_analyst"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)
