from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["New York", "London", "Tokyo"]) & (F.col("dept_name") == "Sales"))
    .groupBy(
        F.when(F.col("budget") >= 4338000, "high").when(F.col("budget") >= 1866000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        F.sum("budget").alias("total_budget"),
    )
    .filter(F.col("total_budget") > 10076000)
    .orderBy(
        F.col("dept_name_values").desc(),
        "total_budget",
        "budget_bucket",
        "dept_name",
    )
    .limit(5)
)
