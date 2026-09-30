from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc())
w2 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("level").orderBy(F.col("salary_avg_5").asc_nulls_last(), "id")
w4 = Window.partitionBy("manager_id", "dept_id")
w5 = Window.partitionBy("job_title").orderBy("salary", "id")
w6 = Window.orderBy("salary_dense_rank", "id")
w7 = Window.partitionBy("manager_id").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("location", "job_title").orderBy(F.col("salary").desc(), "id")
w9 = Window.partitionBy("location").orderBy("salary_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_employees
    .select(
        "*",
        F.dense_rank().over(w).alias("salary_dense_rank"),
        F.avg("salary").over(w2).alias("salary_avg_5"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_dims = (
    latest
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.full_name").alias("employee_full_name"))
)

metrics = (
    with_dims
    .select(
        "*",
        F.round((F.col("salary_avg_5") / F.sum("salary_avg_5").over(w4)), 4).alias("salary_avg_5_share"),
        F.ntile(10).over(w5).alias("salary_bucket"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

joined = (
    metrics
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.dept_name"), F.col("d.budget"), F.col("d.location"))
)

metrics_employees = (
    joined
    .select(
        "*",
        F.first("salary_bucket", ignorenulls=True).over(w7).alias("first_known_salary_bucket"),
        F.ntile(3).over(w8).alias("salary_bucket_2"),
        F.first("salary_avg_5_share", ignorenulls=True).over(w9).alias("first_known_salary_avg_5_share"),
    )
)

result = (
    metrics_employees
    .groupBy("manager_id", "location")
    .pivot("job_title", ["Intern", "Designer", "Director"])
    .agg(
        F.sum("salary_avg_5_share").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "manager_id",
        "location",
        F.col("Intern_value_sum").alias("intern_value_sum"),
        F.col("Intern_item_count").alias("intern_item_count"),
        F.col("Designer_value_sum").alias("designer_value_sum"),
        F.col("Designer_item_count").alias("designer_item_count"),
        F.col("Director_value_sum").alias("director_value_sum"),
        F.col("Director_item_count").alias("director_item_count"),
    )
)
