from pyspark.sql import functions as F

base = df_departments.filter(F.col("location") == "Chiang Mai")

dept_name_summary = (
    base
    .groupBy("dept_name")
    .agg(
        F.count("budget").alias("budget_count"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
)

result = (
    dept_name_summary
    .filter(F.col("budget_count") < 6)
    .select("dept_name", "budget_count")
    .orderBy("dept_name")
)
