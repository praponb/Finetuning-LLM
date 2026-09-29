from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (F.col("hire_date") > F.to_date(F.lit("2023-05-07"))))
    .select(
        "id",
        "dept_id",
        "hire_date",
        "is_remote",
        "level",
        F.initcap(F.trim("job_title")).alias("job_title_title"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("month", "hire_date").alias("activity_month"), "dept_id")
    .agg(
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_rows"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
    )
    .filter(F.col("l2_count") >= 1)
    .orderBy(F.col("dept_id").desc_nulls_first())
)
