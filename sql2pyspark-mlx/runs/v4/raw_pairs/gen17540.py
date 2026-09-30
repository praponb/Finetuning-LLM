from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "London") & (F.col("location") == "Chiang Mai"))
    .groupBy("dept_name", "location")
    .agg(F.sum("budget").alias("total_budget"))
    .filter(F.col("total_budget") > 12499000)
    .orderBy(F.col("total_budget").desc(), "dept_name", "location")
    .limit(20)
)
