from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id")
w2 = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("manager_id").orderBy("next_salary_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_id").orderBy("salary_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_employees
    .filter(F.col("dept_id").isin([1, 9]))
    .withColumn("salary_int", F.col("salary").cast("int"))
    .withColumn("job_title_clean", F.coalesce("job_title", F.lit("NONE")))
)

enriched = (
    base
    .select(
        "*",
        F.lead("salary_int", 1).over(w).alias("next_salary_int"),
        F.sum("salary_int").over(w2).alias("running_salary_int"),
    )
)

joined = (
    enriched
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.budget"))
)

scored = (
    joined
    .select(
        "*",
        F.last("running_salary_int", ignorenulls=True).over(w3).alias("last_known_running_salary_int"),
        F.first("salary_int", ignorenulls=True).over(w4).alias("first_known_salary_int"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("budget") >= 4369000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.round(F.avg("salary_int"), 2).alias("avg_salary_int"),
        F.round(F.avg("running_salary_int"), 2).alias("avg_running_salary_int"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_rows"),
    )
)
