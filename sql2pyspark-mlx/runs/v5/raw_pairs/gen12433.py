from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("salary_tier").orderBy(F.col("l4_count").desc())

kept_keys = (
    df_employees
    .select("job_title", "manager_id")
    .subtract(
        df_employees
        .filter(F.col("job_title") != "Manager")
        .select("job_title", "manager_id")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("salary") >= 130000, "LARGE").otherwise("SMALL").alias("salary_tier"),
        "job_title",
    )
    .agg(
        F.count_if(F.col("job_title") == "Intern").alias("intern_events"),
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_count"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"),
    )
    .filter(F.col("intern_events") > 1)
    .withColumn("l4_count_rank", F.rank().over(w))
)
