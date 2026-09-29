from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Sales") & ~F.col("dept_name").isin(["Marketing", "Legal", "HR"]) & (F.col("budget") >= 3136000))
    .groupBy("dept_name")
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 2)
)
