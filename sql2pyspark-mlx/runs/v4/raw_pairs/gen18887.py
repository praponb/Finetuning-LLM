from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc_nulls_last(), "id")
w_part = Window.partitionBy("dept_id").orderBy("salary", "id")
w3 = Window.partitionBy("manager_id")

base = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-05-13").cast("date")) & F.col("job_title").isNotNull() & F.col("full_name").isNotNull())
    .withColumn("level_filled", F.coalesce("level", F.lit("unassigned")))
)

ranked = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_dims = (
    ranked
    .alias("src")
    .join(df_employees.alias("b"), F.col("src.manager_id") == F.col("b.id"), "left")
    .select(
        "src.*",
        F.col("b.full_name").alias("employee_full_name"),
        F.col("b.hire_date").alias("employee_hire_date"),
        F.col("b.level").alias("employee_level"),
    )
)

metrics = (
    with_dims
    .select(
        "*",
        F.lag("salary", 1, 0).over(w_part).alias("prev_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
    )
)

result = (
    metrics
    .rollup("dept_id", "job_title")
    .agg(
        F.size(F.collect_set("level")).alias("num_levels"),
        F.stddev_samp("salary").alias("salary_stddev"),
        F.grouping("dept_id").alias("grouping_id"),
    )
    .orderBy(F.col("num_levels").desc_nulls_last())
)
