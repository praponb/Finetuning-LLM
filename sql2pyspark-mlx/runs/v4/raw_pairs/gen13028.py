from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location", "dept_name")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_legal"),
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
    )
    .filter(F.col("pct_legal") >= 51)
    .select("location", "dept_name", "pct_legal", "london_budget")
    .orderBy(F.col("pct_legal").desc(), "dept_name")
)
