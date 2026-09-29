from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("budget") < 2063000)
    .select(
        "*",
        F.concat_ws("/", "location", "dept_name").alias("location_dept_name_key"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id").isin([4, 5, 32, 33])), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.salary") <= 108000), "left_semi")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("budget") >= 3936000, "HIGH").otherwise("LOW").alias("budget_tier"),
        "location",
    )
    .agg(
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("budget"))).alias("sales_total_budget"),
    )
)
