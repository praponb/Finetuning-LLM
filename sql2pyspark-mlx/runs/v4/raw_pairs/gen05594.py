from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & F.col("dept_name").isin(["Sales", "Engineering"]))
    .withColumn("dept_name_title", F.initcap(F.trim("dept_name")))
)

latest_match = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"))
)

cleaned = (
    latest_match
    .filter(F.col("dept_name").isNotNull())
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "hire_date",
        F.col("budget").cast("int").alias("budget_int"),
    )
)

result = (
    cleaned
    .withColumn("year_start", F.date_trunc("YEAR", "hire_date"))
    .groupingSets([["location", "year_start"], ["location"]], "location", "year_start")
    .agg(
        F.percentile_approx("budget", 0.95).alias("budget_p95"),
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_rows"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Sales", 1)), F.lit(0)), 2).alias("budget_per_sales"),
        F.percentile_approx("budget", 0.95).alias("budget_p95_2"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.avg(F.when(F.col("location") == "Berlin", F.col("budget_int"))).alias("berlin_avg_budget_int"),
    )
)
