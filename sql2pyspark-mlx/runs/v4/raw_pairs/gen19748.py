from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2024-01-10").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "hire_date",
        "is_remote",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)

joined = (
    valid
    .alias("e")
    .join(df_employees.alias("b"), F.col("e.manager_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.level"))
)

with_windows = (
    joined
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.first("level", ignorenulls=True).over(w2).alias("first_known_level"),
    )
)

result = (
    with_windows
    .groupBy("level", F.date_trunc("MONTH", "hire_date").alias("activity_month"))
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
        F.sort_array(F.collect_set("level")).alias("level_list_2"),
    )
)
