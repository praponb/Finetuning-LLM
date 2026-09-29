from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("budget") > 1550000))
    .groupBy("location")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_legal"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 5)
)
