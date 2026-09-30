from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc(), "id")
w3 = Window.partitionBy("dept_id", "location").orderBy("hire_date", "id")
w_running = Window.partitionBy("location", "dept_id").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_employees
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "dept_id", "manager_id", "salary", "hire_date", "is_remote")
)

enriched = (
    ranked
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.dept_name").isin(["Legal", "Sales", "Marketing"]))
    .select("s.id", "s.dept_id", "s.hire_date", F.col("d.budget"), F.col("d.location"))
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched_employees = (
    latest
    .select(
        "*",
        F.lead("budget", 1).over(w3).alias("next_budget"),
        F.sum("budget").over(w_running).alias("running_budget"),
    )
)

result = (
    enriched_employees
    .select("id", "location", "next_budget", "budget")
    .unpivot(
        ids=["id", "location"],
        values=["next_budget", "budget"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
