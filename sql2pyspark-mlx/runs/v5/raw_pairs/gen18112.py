from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget_rounded_scaled", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("budget_rounded_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_departments
    .filter(F.col("budget") > 1561000)
    .select("*", F.round("budget", 2).alias("budget_rounded"))
)

filtered_departments = (
    filtered
    .filter(F.col("dept_name").isNotNull())
    .select(
        "id",
        "dept_name",
        "location",
        "budget_rounded",
        F.when(F.col("budget_rounded") >= 79, "High").when(F.col("budget_rounded") >= 29, "Medium").otherwise("Low").alias("budget_rounded_band"),
        F.round((F.col("budget_rounded") * 0.85), 2).alias("budget_rounded_scaled"),
        F.coalesce("dept_name", F.lit("DIRECT")).alias("resolved_dept_name"),
    )
)

metrics = (
    filtered_departments
    .select(
        "*",
        F.min("budget_rounded_scaled").over(w).alias("last3_min_budget_rounded_scaled"),
        F.avg("budget_rounded_scaled").over(w).alias("last3_avg_budget_rounded_scaled"),
        F.sum("budget_rounded_scaled").over(w).alias("last3_sum_budget_rounded_scaled"),
        F.sum("budget_rounded").over(w2).alias("running_budget_rounded"),
    )
)

filtered_departments_2 = (
    metrics
    .filter(F.col("dept_name").isin(["HR", "Marketing", "Legal"]))
    .select(
        "id",
        "dept_name",
        "budget_rounded",
        "budget_rounded_band",
        "budget_rounded_scaled",
        "resolved_dept_name",
        "last3_min_budget_rounded_scaled",
        "last3_avg_budget_rounded_scaled",
        "last3_sum_budget_rounded_scaled",
        "running_budget_rounded",
        F.round((F.col("last3_avg_budget_rounded_scaled") * 1.1), 2).alias("last3_avg_budget_rounded_scaled_scaled"),
    )
)

result = (
    filtered_departments_2
    .select(
        "id",
        "budget_rounded",
        "budget_rounded_band",
        "budget_rounded_scaled",
        "resolved_dept_name",
        "last3_sum_budget_rounded_scaled",
        "running_budget_rounded",
        "last3_avg_budget_rounded_scaled_scaled",
    )
    .orderBy(F.col("budget_rounded").desc())
)
