from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")

ranked = (
    df_employees
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    ranked
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .filter(~F.col("d.location").isin(["Berlin", "Tokyo", "London"]))
    .select("a.*", F.col("d.location"), F.col("d.budget"))
)

joined = (
    enriched
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.location").alias("department_location"),
        F.col("d.dept_name"),
    )
)

deduped = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched_employees = (
    deduped
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.dept_name").alias("department_dept_name"),
        F.col("p.location").alias("department_location_2"),
        F.col("p.budget").alias("department_budget"),
    )
)

result = (
    enriched_employees
    .groupBy(
        F.when(F.col("department_budget") >= 3599000, "PREMIUM").otherwise("STANDARD").alias("department_budget_tier"),
    )
    .agg(
        F.avg(F.when(F.col("location") == "New York", F.col("department_budget"))).alias("new_york_avg_department_budget"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("job_title") == "Manager", 1)), F.lit(0)), 2).alias("budget_per_manager"),
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"),
    )
    .orderBy(F.col("new_york_avg_department_budget").desc_nulls_first())
)
