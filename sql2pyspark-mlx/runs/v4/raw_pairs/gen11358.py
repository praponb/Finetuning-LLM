from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

valid = (
    df_departments
    .filter(F.col("location").isNotNull() & F.col("budget").between(1385000, 2717000))
    .select(
        "*",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.coalesce("dept_name", F.lit("unassigned")).alias("dept_name_filled"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.level") == "L5"), "left_anti")
)

filtered = (
    eligible
    .filter(~F.col("location").isin(["Bangkok", "London"]))
    .select(
        "*",
        F.trim("location").alias("clean_location"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("dept_name_filled_2"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

filtered_departments = (
    ranked
    .filter(F.col("location").isNotNull())
    .withColumn("budget_whole", F.floor("budget"))
)

cleaned = (
    filtered_departments
    .filter(F.col("dedup_rank") < 1)
    .select(
        "*",
        F.when(F.col("location").isin(["Tokyo", "London"]), "Priority").otherwise("Other").alias("location_group"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("budget_filled") >= 72, "HIGH").otherwise("LOW").alias("budget_filled_tier"),
    )
    .agg(
        F.count_if(F.col("location") == "Singapore").alias("singapore_events"),
        F.count_if(F.col("location") == "London").alias("london_events"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
    )
    .filter(F.col("singapore_events") >= 1)
    .orderBy(F.col("london_events").desc())
)
