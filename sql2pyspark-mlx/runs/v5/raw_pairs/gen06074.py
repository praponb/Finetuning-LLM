from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w2 = Window.orderBy(F.col("budget").asc_nulls_first())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("employee_level").orderBy(F.col("max_budget_share").desc())

base = (
    df_departments
    .filter(F.col("budget") > 3759000)
    .select(
        "*",
        F.when(F.col("budget") >= 3480000, "High").when(F.col("budget") >= 831000, "Medium").otherwise("Low").alias("budget_level"),
        F.substring("dept_name", 1, 2).alias("dept_name_prefix"),
        F.coalesce("location", F.lit("UNKNOWN")).alias("location_clean"),
    )
)

enriched = (
    base
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w)), 4))
    .withColumn("budget_rank", F.rank().over(w2))
)

valid = (
    enriched
    .filter(F.col("location").isNotNull())
    .withColumn("dept_name_prefix_2", F.substring("dept_name", 1, 4))
)

latest_match = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.budget_level",
        "s.budget_share",
        "s.budget_rank",
        F.col("c.level").alias("employee_level"),
    )
)

result = (
    latest_match
    .groupBy(
        "employee_level",
        F.when(F.col("budget_share") >= 0.6, "HIGH").otherwise("LOW").alias("budget_share_tier"),
        F.when(F.col("budget_rank") >= 7, "LARGE").otherwise("SMALL").alias("budget_rank_tier"),
    )
    .agg(
        F.max(F.when(F.col("employee_level") == "L1", F.col("budget_rank"))).alias("l1_max_budget_rank"),
        F.max("budget_share").alias("max_budget_share"),
        F.sum(F.when(F.col("employee_level") == "L1", F.col("budget_rank")).otherwise(0)).alias("l1_budget_rank"),
        F.count(F.when(F.col("employee_level") == "L3", 1)).alias("l3_count"),
        F.count(F.when(F.col("employee_level") == "L4", 1)).alias("l4_count"),
        F.count(F.when(F.col("employee_level") == "L3", 1)).alias("l3_count_2"),
    )
    .withColumn("max_budget_share_percent_rank", F.percent_rank().over(w4))
    .orderBy(F.col("l3_count").desc())
)
