from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location").orderBy(F.col("salary").desc(), "id")
w3 = Window.orderBy(F.col("budget_whole").desc(), "id")

base = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Chiang Mai", "Bangkok"]))
    .withColumn("budget_whole", F.floor("budget"))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("DIRECT")))
)

with_latest = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"))
)

scored = (
    with_latest
    .withColumn("salary_bucket", F.ntile(5).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)

kept = (
    scored
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.email").like("%@outlook.com")), "left_semi")
)

matched_keys = (
    kept
    .select("dept_name", "location")
    .intersectAll(
        df_departments
        .filter(F.col("budget") < 3305000)
        .select("dept_name", "location")
    )
)

kept_rows = (
    kept
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

stats = (
    kept_rows
    .groupBy(
        F.when(F.col("budget") >= 3368000, "LARGE").otherwise("SMALL").alias("budget_tier"),
        F.when(F.col("budget_whole") >= 75, "HIGH").otherwise("LOW").alias("budget_whole_tier"),
    )
    .agg(
        F.round(F.sum("salary_bucket") / F.nullif(F.count(F.when(F.col("dept_name") == "HR", 1)), F.lit(0)), 2).alias("salary_bucket_per_hr"),
        F.max(F.when(F.col("location") == "Tokyo", F.col("salary_bucket"))).alias("tokyo_max_salary_bucket"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
    )
)

result = (
    stats
    .select(
        "budget_tier",
        "budget_whole_tier",
        "salary_bucket_per_hr",
        "tokyo_max_salary_bucket",
        "num_dept_names",
    )
)
