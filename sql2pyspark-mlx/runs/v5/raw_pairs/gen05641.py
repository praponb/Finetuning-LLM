from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("employee_level").orderBy(F.col("budget").desc_nulls_last())
w3 = Window.partitionBy("dept_name").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("employee_level").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.month(F.col("c.hire_date")).isin([8, 9, 12])), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level").alias("employee_level"), F.col("c.salary"))
)

scored = (
    latest_match
    .select(
        "id",
        "location",
        "budget",
        "employee_level",
        "salary",
        F.cume_dist().over(w2).alias("budget_cume_dist"),
        F.sum("salary").over(w3).alias("running_salary"),
        F.last("location", ignorenulls=True).over(w4).alias("last_known_location"),
    )
)

base = (
    scored
    .filter(F.col("employee_level").isNotNull() & F.col("running_salary").between(334395, 699360))
    .withColumn("salary_times_budget", (F.col("salary") * F.col("budget")))
)

result = (
    base
    .select(
        "id",
        "location",
        "employee_level",
        "salary",
        "budget_cume_dist",
        "running_salary",
        "last_known_location",
        F.when(F.col("location").rlike("^(New York|Bangkok)$"), "CORE").when(F.col("location").rlike("^(Chiang Mai|Singapore)$"), "GROUP_A").otherwise("GROUP_B").alias("location_zone"),
    )
    .orderBy(F.col("salary").desc_nulls_first(), "budget_cume_dist")
)
