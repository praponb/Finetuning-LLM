from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name", "location")
    .agg(
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("location") == "London", 1)), F.lit(0)), 2).alias("budget_per_london"),
        F.sum("budget").alias("total_budget"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list_2"),
    )
)
