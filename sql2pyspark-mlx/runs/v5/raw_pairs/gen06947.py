from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "dept_id").orderBy(F.col("hire_date").asc_nulls_first(), "id")
w2 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())
w3 = Window.partitionBy("manager_id").orderBy("salary", "id")

deduped = (
    df_employees
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "email", "manager_id", "salary", "is_remote")
)

with_windows = (
    deduped
    .select(
        "*",
        F.percent_rank().over(w2).alias("salary_percent_rank"),
        F.lead("salary", 1, 0).over(w3).alias("next_salary"),
    )
)

with_dims = (
    with_windows
    .alias("e")
    .join(df_employees.alias("p2"), F.col("e.manager_id") == F.col("p2.id"), "inner")
    .select(
        "e.id",
        "e.manager_id",
        "e.salary_percent_rank",
        "e.next_salary",
        F.col("p2.hire_date"),
        F.col("p2.level"),
        F.col("p2.email").alias("employee_email"),
    )
)

cleaned = (
    with_dims
    .filter((F.col("hire_date") > F.lit("2023-04-14").cast("date")) & (~F.col("level").eqNullSafe(F.lit("L2"))))
    .select(
        "*",
        F.when(F.col("level") == "L1", 1).otherwise(0).alias("is_l1"),
        F.coalesce("level", F.lit("NONE")).alias("level_filled"),
    )
)

grouped = (
    cleaned
    .groupBy(
        F.date_trunc("year", "hire_date").alias("year_start"),
        F.date_trunc("week", "hire_date").alias("week_start"),
    )
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.countDistinct("manager_id").alias("distinct_manager_ids"),
        F.round(F.sum("is_l1") / F.nullif(F.count(F.when(F.col("level") == "L2", 1)), F.lit(0)), 2).alias("is_l1_per_l2"),
        F.sum(F.when(F.col("level") == "L5", F.col("is_l1")).otherwise(0)).alias("l5_is_l1"),
        F.count_if(F.col("level") == "L3").alias("l3_events"),
    )
)

result = (
    grouped
    .select(
        "year_start",
        "week_start",
        "level_list",
        "distinct_manager_ids",
        "is_l1_per_l2",
        "l5_is_l1",
    )
)
