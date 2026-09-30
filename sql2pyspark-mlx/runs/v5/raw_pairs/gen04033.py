from pyspark.sql import functions as F

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.hire_date") <= "2025-05-01"), "left_semi")
)

result = (
    eligible
    .withColumn("budget_tier", F.when(F.col("budget") >= 3496000, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["dept_name", "budget_tier"], ["dept_name"], []],
        "dept_name",
        "budget_tier",
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count("*").alias("event_count"),
        F.grouping_id().alias("grouping_level"),
    )
)
