from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("location_length", F.length("location"))
    .withColumn("adjusted_budget", F.round((F.col("budget") * 1.07), 2))
    .withColumn("location_clean", F.coalesce("location", F.lit("N/A")))
)

result = (
    valid
    .select(
        "id",
        "dept_name",
        F.col("budget").alias("BUDGET"),
        F.col("adjusted_budget").alias("ADJUSTED_BUDGET_VAL"),
    )
    .unpivot(["id", "dept_name"], ["BUDGET", "ADJUSTED_BUDGET_VAL"], "kpi", "value")
    .filter(F.col("value").isNotNull())
)
