from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location")
w2 = Window.orderBy("budget", "id")
w3 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("location")
w5 = Window.partitionBy("dept_name")
w6 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc(), "id")

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & F.col("dept_name").isin(["Engineering", "Sales", "Marketing"]))
    .withColumn("location_lower", F.lower("location"))
    .withColumn("location_filled", F.coalesce("location", F.lit("UNKNOWN")))
)

metrics = (
    valid
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w)), 4))
    .withColumn("next_budget", F.lead("budget", 2).over(w2))
    .withColumn("first_known_location", F.first("location", ignorenulls=True).over(w3))
)

kept = (
    metrics
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.length(F.col("ch.full_name")) > 8), "left_semi")
)

kept_departments = (
    kept
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.length(F.col("x.email")) >= 9), "left_semi")
)

result = (
    kept_departments
    .select(
        "*",
        F.round((F.col("next_budget") / F.sum("next_budget").over(w4)), 4).alias("next_budget_share"),
        F.avg("budget").over(w5).alias("dept_name_avg_budget"),
        F.row_number().over(w6).alias("seq_no"),
    )
)
