from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("location", "dept_name").orderBy(F.col("row_num").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

base = (
    df_departments
    .filter(~F.col("location").isin(["Tokyo", "Bangkok"]))
    .withColumn("budget_tier", F.when(F.col("budget") >= 3206000, "High").when(F.col("budget") >= 1297000, "Medium").otherwise("Low"))
    .withColumn("location_filled", F.coalesce("location", F.lit("DIRECT")))
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

base_departments = (
    latest
    .filter(F.col("location").isin(["New York", "Singapore", "Tokyo"]))
    .select(
        "id",
        "dept_name",
        "location",
        "budget_tier",
        "location_filled",
        "row_num",
        F.round((F.col("budget") * 0.9), 2).alias("adjusted_budget"),
        (F.col("budget") * F.col("row_num")).alias("budget_times_row_num"),
        F.coalesce("location", F.lit("N/A")).alias("location_clean"),
    )
)

ranked = (
    base_departments
    .withColumn("row_num_2", F.row_number().over(w2))
    .filter(F.col("row_num_2") == 1)
    .drop("row_num_2")
)

latest_match = (
    ranked
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"), F.col("c.level").alias("employee_level"))
)

daily = (
    latest_match
    .groupBy(
        F.date_trunc("WEEK", "hire_date").alias("week_start"),
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("location") == "Bangkok", F.col("budget_times_row_num")).otherwise(0)).alias("bangkok_budget_times_row_num"),
        F.count(F.when(F.col("employee_level") == "L1", 1)).alias("l1_count"),
        F.size(F.collect_set("location")).alias("num_locations"),
        F.round(F.avg("budget_times_row_num"), 2).alias("avg_budget_times_row_num"),
        F.percentile_approx("budget_times_row_num", [0.25, 0.5, 0.75]).alias("budget_times_row_num_quartiles"),
    )
)

result = (
    daily
    .select(
        "week_start",
        "quarter_start",
        "event_count",
        "bangkok_budget_times_row_num",
        "l1_count",
        "num_locations",
    )
    .orderBy(F.col("quarter_start").desc(), F.col("l1_count").desc())
)
