from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") != "Berlin") & F.col("location").isNull())
    .groupBy(
        F.when(F.col("budget") >= 3399000, "high").when(F.col("budget") >= 966000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        F.sum("budget").alias("total_budget"),
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Sales", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sales"),
    )
    .filter(F.col("pct_sales") > 58)
    .orderBy("pct_sales", "budget_bucket")
    .limit(10)
)
