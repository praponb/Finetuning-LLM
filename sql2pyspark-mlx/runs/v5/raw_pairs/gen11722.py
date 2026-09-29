from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w_grp = Window.partitionBy("location").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "Finance", "Marketing"]))
    .withColumn("dept_name_lower", F.lower("dept_name"))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("NONE")))
)

kept = (
    cleaned
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.year(F.col("ch.hire_date")) == 2025), "left_anti")
)

with_latest = (
    kept
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"))
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.dept_id").isin([1, 26, 28])), "left_anti")
)

cleaned_departments = (
    not_flagged
    .filter(F.col("location") != "New York")
    .withColumn("dept_name_prefix", F.substring("dept_name", 1, 2))
    .withColumn("resolved_location", F.coalesce("location", F.lit("DIRECT")))
)

scored = (
    cleaned_departments
    .withColumn("running_sum_salary", F.sum("salary").over(w_grp))
    .withColumn("running_min_salary", F.min("salary").over(w_grp))
    .withColumn("running_count_salary", F.count("salary").over(w_grp))
)

daily = (
    scored
    .groupBy("dept_name")
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.sum(F.when(F.col("location") == "Chiang Mai", F.col("salary")).otherwise(0)).alias("chiang_mai_salary"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.count_if(F.col("location") == "Tokyo").alias("tokyo_events"),
        F.count("*").alias("event_count"),
    )
)

result = (
    daily
    .select(
        "dept_name",
        "chiang_mai_salary",
        "distinct_dept_names",
        "tokyo_events",
        "event_count",
    )
    .orderBy(F.col("distinct_dept_names").desc_nulls_first(), "dept_name")
)
