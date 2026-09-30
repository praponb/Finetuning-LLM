from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("level", "dept_name").orderBy(F.col("employee_salary").desc(), "id")
w3 = Window.partitionBy("dept_name")
w4 = Window.orderBy("budget", "id")
w5 = Window.partitionBy("level").orderBy(F.col("employee_salary").desc())

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select("*", F.coalesce("location", F.lit("N/A")).alias("location_filled"))
)

latest_match = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"), F.col("c.salary").alias("employee_salary"))
)

latest = (
    latest_match
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    latest
    .withColumn("employee_salary_share", F.round((F.col("employee_salary") / F.sum("employee_salary").over(w3)), 4))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("employee_salary_cume_dist", F.cume_dist().over(w5))
)

result = (
    metrics
    .withColumn("employee_salary_share_tier", F.when(F.col("employee_salary_share") >= 0.72, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["seq_no", "employee_salary_share_tier", "dept_name"], ["seq_no", "employee_salary_share_tier"], []],
        "seq_no",
        "employee_salary_share_tier",
        "dept_name",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_count"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.grouping("seq_no").alias("grouping_id"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy("employee_salary_share_tier")
)
