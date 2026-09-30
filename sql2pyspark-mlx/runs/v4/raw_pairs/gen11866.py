from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w3 = Window.orderBy(F.col("employee_salary").desc(), "id")
w4 = Window.partitionBy("level", "location").orderBy(F.col("adjusted_budget").desc(), "id")
w5 = Window.partitionBy("location").orderBy("adjusted_budget", "id").rowsBetween(-5, Window.currentRow)

filtered = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "London"]))
    .select(
        "*",
        F.round((F.col("budget") * 1.07), 2).alias("adjusted_budget"),
        F.trim("location").alias("clean_location"),
        F.coalesce("location", F.lit("UNKNOWN")).alias("location_clean"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary").alias("employee_salary"), F.col("c.level"))
)

with_latest_departments = (
    with_latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary").alias("employee_salary_2"))
)

enriched = (
    with_latest_departments
    .withColumn("employee_salary_bucket", F.ntile(3).over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("employee_salary_2_avg_5", F.avg("employee_salary_2").over(w5))
)

result = (
    enriched
    .groupBy("dept_name", "level")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.max(F.when(F.col("level") == "L5", F.col("row_seq"))).alias("l5_max_row_seq"),
        F.sum(F.when(F.col("location") == "Chiang Mai", F.col("employee_salary_bucket")).otherwise(0)).alias("chiang_mai_employee_salary_bucket"),
    )
    .orderBy(F.col("l5_max_row_seq").desc_nulls_first())
)
