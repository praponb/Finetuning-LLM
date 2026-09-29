from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L3", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l3"),
    )
    .filter(F.col("pct_l3") < 41)
)
