from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") <= 2029000) & ((F.col("location") != "Chiang Mai") | (F.col("dept_name") == "HR")))
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0)).alias("engineering_count"),
    )
    .filter(F.col("engineering_count") > 60)
    .orderBy(F.col("dept_name").desc())
)
