from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary")
w2 = Window.partitionBy("job_title", "level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("job_title")
w_ordered = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc_nulls_last(), "id")
w6 = Window.partitionBy("dept_name").orderBy("salary", "id")

with_windows = (
    df_employees
    .select(
        "*",
        F.dense_rank().over(w).alias("salary_dense_rank"),
        F.first("level", ignorenulls=True).over(w2).alias("first_known_level"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
        F.avg("salary").over(w_ordered).alias("salary_avg_5_prior"),
    )
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

filtered = (
    latest
    .filter(F.col("hire_date") >= F.to_date(F.lit("2024-03-25")))
    .withColumn("hire_date_plus_months", F.add_months("hire_date", 12))
)

joined = (
    filtered
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.dept_name"), F.col("d.budget"))
)

with_dims = (
    joined
    .alias("a")
    .join(df_departments.alias("b"), F.col("a.dept_id") == F.col("b.id"), "left")
    .select(
        "a.*",
        F.col("b.budget").alias("department_budget"),
        F.col("b.dept_name").alias("department_dept_name"),
    )
)

deduped = (
    with_dims
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

matched_keys = (
    deduped
    .select("dept_id")
    .intersectAll(
        df_employees
        .filter(F.col("dept_id") == 21)
        .select("dept_id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .cube(F.date_trunc("WEEK", "hire_date_plus_months").alias("week_start"), "dept_id")
    .agg(
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("salary_share") / F.nullif(F.count(F.when(F.col("dept_name") == "Sales", 1)), F.lit(0)), 2).alias("salary_share_per_sales"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("department_dept_name") == "Sales", 1)).alias("sales_rows"),
        F.max(F.when(F.col("job_title") == "Analyst", F.col("salary"))).alias("analyst_max_salary"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("analyst_max_salary").desc_nulls_last())
)
