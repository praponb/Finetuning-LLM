from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter(F.col("location").isin(["Tokyo", "Chiang Mai", "Berlin"]) & F.col("dept_name").isin(["Legal", "Sales", "Marketing"]))
)

summary = (
    valid_departments
    .groupBy("dept_name")
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count(F.when(F.col("location") == "London", 1)).alias("london_count"),
    )
)

result = summary.orderBy(F.col("london_count").desc(), F.col("avg_budget").desc())
