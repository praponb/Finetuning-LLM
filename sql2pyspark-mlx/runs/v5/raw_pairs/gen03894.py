from pyspark.sql import functions as F

result = (
    df_departments
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0))) / F.count("*")), 1).alias("pct_finance"),
    )
    .orderBy("pct_finance")
)
