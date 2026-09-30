from pyspark.sql import functions as F

result = (
    df_employees
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L4", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l4"),
    )
    .orderBy("pct_l4")
)
