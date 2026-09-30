from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
        F.sum(F.when(F.col("dept_name") == "Marketing", F.col("budget")).otherwise(0)).alias("marketing_budget"),
    )
    .filter(F.col("pct_bangkok") >= 21)
    .orderBy(F.col("pct_bangkok").desc())
)
