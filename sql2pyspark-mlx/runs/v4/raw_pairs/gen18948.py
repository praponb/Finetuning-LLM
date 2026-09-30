from pyspark.sql import functions as F

result = (
    df_departments
    .filter(((F.col("location") == "New York") & ~F.col("location").isin(["New York", "Singapore", "Berlin"])) | F.col("location").isin(["London", "Tokyo", "Chiang Mai"]))
    .groupBy("dept_name")
    .agg(
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_count"),
        F.round(((100 * F.sum(F.when(F.col("location") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
    )
    .orderBy(F.col("singapore_count").desc(), "dept_name")
    .limit(3)
)
