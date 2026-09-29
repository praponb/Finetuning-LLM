from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc_nulls_first(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

filtered = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "Chiang Mai"]))
    .select(
        "*",
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
        F.concat_ws("/", "dept_name", "location").alias("dept_name_location_key"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("dept_name_clean"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

deduped = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

latest_match = (
    deduped
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"), F.col("c.level").alias("employee_level"))
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("salary") >= 138000, "HIGH").otherwise("LOW").alias("salary_tier"),
    )
    .agg(
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.size(F.collect_set("employee_level")).alias("num_employee_levels"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("num_employee_levels") >= 1)
)
