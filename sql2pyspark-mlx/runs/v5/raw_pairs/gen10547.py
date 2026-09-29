from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(~((F.col("salary") >= 120000) & ~F.col("level").isin(["L2", "L4"])))
    .withColumn("is_l5", F.when(F.col("level") == "L5", 1).otherwise(0))
)

summary = (
    filtered_employees
    .groupBy("is_remote")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L4", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l4"),
    )
)

result = (
    summary
    .filter(F.col("pct_l4") >= 67)
    .orderBy(F.col("pct_l4").desc())
)
