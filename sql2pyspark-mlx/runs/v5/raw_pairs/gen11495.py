from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_id", "level").orderBy("hire_date", "id")
w3 = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy("job_title", "level").orderBy(F.col("salary").desc(), "id")
w5 = Window.partitionBy("level").orderBy(F.col("cumulative_salary").desc(), "id")

filtered = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-02-24").cast("date"))
    .select(
        "*",
        F.concat_ws(",", "level", "full_name").alias("level_full_name_key"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("job_title", F.lit("NONE")).alias("resolved_job_title"),
    )
)

metrics = (
    filtered
    .filter(F.col("email").isNotNull())
    .withColumn("cumulative_salary", F.sum("salary").over(w_ordered))
    .withColumn("prev_salary", F.lag("salary", 1).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)

deduped = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        "is_remote_label",
        "cumulative_salary",
    )
)

with_dims = (
    deduped
    .alias("e")
    .join(df_departments.alias("p"), F.col("e.dept_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.location"))
)

latest = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

matched_keys = (
    latest
    .select("level", "dept_id")
    .subtract(
        df_employees
        .filter(F.col("dept_id").isin([10, 11, 13, 24]))
        .select("level", "dept_id")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote_label",
        "cumulative_salary",
        "location",
    )
    .orderBy("hire_date", "dept_id")
)
