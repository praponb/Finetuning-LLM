from pyspark.sql import functions as F

src = df_employees.filter((F.col("salary") > 148000) & (F.col("salary") > 111000))

employees_stats = (
    src
    .groupBy(
        F.when(F.col("level").isin(["L4", "L5"]), "tier_1").otherwise("other").alias("level_group"),
        F.date_trunc("MONTH", "hire_date").alias("hire_month"),
    )
    .agg(
        F.sum(F.when(F.col("level") == "L5", F.col("salary")).otherwise(0)).alias("l5_salary"),
    )
)

result = (
    employees_stats
    .filter(F.col("l5_salary") < 45)
    .select("level_group", "l5_salary")
    .orderBy(F.col("l5_salary").desc())
)
