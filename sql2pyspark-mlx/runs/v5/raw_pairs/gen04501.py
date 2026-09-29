from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 3944000)
    .groupBy("location")
    .agg(
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_count"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
    .filter(F.col("legal_count") > 7)
    .select("location", "legal_count", "stddev_budget")
    .orderBy(F.col("legal_count").desc())
)
