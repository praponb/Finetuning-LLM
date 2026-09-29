from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("employee_level").orderBy(F.col("budget").desc_nulls_first())
w3 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("budget").between(1681000, 3120000) & F.col("location").isNotNull())
    .withColumn("dept_name_location", F.concat("dept_name", F.lit("/"), "location"))
)

latest_match = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"), F.col("c.level").alias("employee_level"))
)

kept = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") < F.col("s.hire_date")), "left_semi")
)

scored = (
    kept
    .filter(~F.col("dept_name").isin(["Sales", "HR", "Legal"]))
    .select(
        "id",
        "budget",
        "dept_name_location",
        "hire_date",
        F.dense_rank().over(w2).alias("budget_dense_rank"),
        F.sum("budget").over(w3).alias("cumulative_budget"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") < F.col("s.hire_date")), "left_anti")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") <= F.col("s.hire_date")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title").alias("employee_job_title"), F.col("c.level"))
)

pivoted = (
    with_last
    .groupBy("employee_job_title")
    .pivot("level", ["L2", "L4", "L1"])
    .agg(F.sum("budget_dense_rank").alias("amount"), F.count(F.lit(1)).alias("n"))
)

result = (
    pivoted
    .select(
        "employee_job_title",
        "L4_n",
        "L1_amount",
        "L1_n",
        F.when(F.col("employee_job_title").rlike("^(Manager|Director)$"), "PRIORITY").when(F.col("employee_job_title").rlike("^(Engineer|Analyst)$"), "GROUP_B").otherwise("GROUP_A").alias("employee_job_title_zone"),
    )
    .orderBy(F.col("employee_job_title").desc_nulls_first(), "L4_n")
)
