from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "dept_id").orderBy(F.col("salary").desc())
w2 = Window.partitionBy("dept_id").orderBy(F.col("hire_date").asc_nulls_last(), "id")
w_grp = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(-5, Window.currentRow)
w_recent = Window.partitionBy("dept_id").orderBy("salary", "id")
w5 = Window.partitionBy("manager_id", "job_title").orderBy("rn", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("dept_id")
w7 = Window.orderBy(F.col("dept_id_min_salary").asc_nulls_first())

base = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-03-10").cast("date"))
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
    .withColumn("full_name_lower", F.lower("full_name"))
)

ranked = (
    base
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

scored = (
    deduped
    .filter(F.col("salary") < 59000)
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "full_name_lower",
        F.avg("salary").over(w_grp).alias("salary_avg_5"),
        F.ntile(5).over(w_recent).alias("salary_bucket"),
        F.avg("salary").over(w5).alias("salary_avg_5_2"),
        F.min("salary").over(w6).alias("dept_id_min_salary"),
    )
)

kept_keys = (
    scored
    .select("dept_id", "manager_id")
    .intersectAll(
        df_employees
        .filter(F.col("hire_date") < "2024-01-01")
        .select("dept_id", "manager_id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.location"))
)

remaining_keys = (
    enriched
    .select("dept_id", "manager_id")
    .intersect(
        df_employees
        .filter(F.col("dept_id").isin([2, 10, 21, 38]))
        .select("dept_id", "manager_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "salary_bucket",
        "salary_avg_5_2",
        F.cume_dist().over(w7).alias("dept_id_min_salary_cume_dist"),
    )
)
