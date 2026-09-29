from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.size(F.collect_set("location")).alias("num_locations"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Finance", 1)), F.lit(0)), 2).alias("budget_per_finance"),
        F.avg(F.when(F.col("location") == "Tokyo", F.col("budget"))).alias("tokyo_avg_budget"),
    )
    .orderBy(F.col("tokyo_avg_budget").asc_nulls_last())
)
