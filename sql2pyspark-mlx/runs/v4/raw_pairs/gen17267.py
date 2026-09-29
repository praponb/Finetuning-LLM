from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("employee_level").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_employees
    .filter(F.col("hire_date") > F.to_date(F.lit("2023-05-04")))
    .withColumn("is_l3", F.when(F.col("level") == "L3", 1).otherwise(0))
    .withColumn("level_clean", F.coalesce("level", F.lit("UNKNOWN")))
)

with_dims = (
    base
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "inner")
    .select(
        "e.*",
        F.col("p.email").alias("employee_email"),
        F.col("p.job_title").alias("employee_job_title"),
        F.col("p.level").alias("employee_level"),
    )
)

with_windows = (
    with_dims
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("last_known_is_l3", F.last("is_l3", ignorenulls=True).over(w2))
    .withColumn("first_known_job_title", F.first("job_title", ignorenulls=True).over(w_ordered))
)

result = (
    with_windows
    .groupBy(
        "dept_id",
        F.when(F.col("row_seq") >= 7, "HIGH").otherwise("LOW").alias("row_seq_tier"),
        "employee_job_title",
    )
    .agg(
        F.percentile_approx("last_known_is_l3", [0.25, 0.5, 0.75]).alias("last_known_is_l3_quartiles"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list_2"),
    )
    .orderBy("row_seq_tier")
)
