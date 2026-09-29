from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Finance"]))
    .select(
        "*",
        F.when(F.col("budget") >= 3911000, "High").when(F.col("budget") >= 1309000, "Medium").otherwise("Low").alias("budget_tier"),
        F.when(F.col("location") == "New York", 1).otherwise(0).alias("is_new_york"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("budget") >= 3810000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("row_count"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.stddev_samp("budget").alias("budget_stddev"),
    )
    .orderBy(F.col("budget_tier").desc_nulls_last())
)
