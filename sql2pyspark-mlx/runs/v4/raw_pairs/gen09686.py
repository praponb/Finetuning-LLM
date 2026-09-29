from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary_scaled", "id")
w_ordered = Window.orderBy("salary_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered_2 = Window.orderBy(F.col("salary").desc())
w4 = Window.partitionBy("level")
w5 = Window.partitionBy("level", "dept_id").orderBy("salary_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("salary_scaled", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("dept_id", "manager_id").orderBy(F.col("salary_rank").desc(), "id")
w8 = Window.partitionBy("manager_id", "dept_id")

valid = (
    df_employees
    .filter(F.col("job_title").isin(["Intern", "Director"]))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        F.concat("email", F.lit(" "), "level").alias("email_level"),
        F.round((F.col("salary") * 1.07), 2).alias("salary_scaled"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_filled"),
    )
)

metrics = (
    valid
    .withColumn("prev_salary_scaled", F.lag("salary_scaled", 1).over(w))
    .withColumn("last_known_salary_scaled", F.last("salary_scaled", ignorenulls=True).over(w_ordered))
    .withColumn("salary_rank", F.rank().over(w_ordered_2))
    .withColumn("level_max_salary_scaled", F.max("salary_scaled").over(w4))
    .withColumn("first_known_level", F.first("level", ignorenulls=True).over(w5))
)

metrics_employees = (
    metrics
    .withColumn("salary_avg_5", F.avg("salary").over(w6))
    .withColumn("event_seq", F.row_number().over(w7))
)

kept_keys = (
    metrics_employees
    .select("level")
    .intersect(
        df_employees
        .filter(F.col("job_title") == "Designer")
        .select("level")
    )
)

verified = (
    metrics_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("event_seq_share", F.round((F.col("event_seq") / F.sum("event_seq").over(w8)), 4))
    .withColumn("salary_dense_rank", F.dense_rank().over(w_ordered_2))
)

result = (
    enriched
    .groupBy("manager_id", "dept_id")
    .pivot("level", ["L4", "L5"])
    .agg(F.avg("level_max_salary_scaled"))
)
