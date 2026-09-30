from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

with_last = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level").alias("employee_level"), F.col("c.salary"))
)

result = (
    with_last
    .withColumn("salary_tier", F.when(F.col("salary") >= 134000, "LARGE").otherwise("SMALL"))
    .withColumn("budget_tier", F.when(F.col("budget") >= 3885000, "HIGH").otherwise("LOW"))
    .cube("salary_tier", "budget_tier")
    .agg(
        F.max(F.when(F.col("employee_level") == "L1", F.col("salary"))).alias("l1_max_salary"),
        F.count(F.when(F.col("employee_level") == "L1", 1)).alias("l1_count"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("location") == "Bangkok", 1)), F.lit(0)), 2).alias("salary_per_bangkok"),
        F.percentile_approx("salary", 0.95).alias("salary_p95"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.count(F.when(F.col("location") == "London", 1)).alias("london_count"),
        F.grouping("salary_tier").alias("gid"),
    )
    .orderBy(F.col("l1_count").desc_nulls_first())
)
