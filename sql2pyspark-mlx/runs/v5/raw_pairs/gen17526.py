from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3355000, "high").when(F.col("budget") >= 1944000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
        F.sum("budget").alias("total_budget"),
    )
    .filter(F.col("total_budget") > 16584000)
)
