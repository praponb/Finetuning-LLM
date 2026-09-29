from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id")
w2 = Window.partitionBy("job_title", "level").orderBy("hire_date_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("job_title").orderBy(F.col("seq_no").desc_nulls_last(), "id")

valid = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-03-04"))) & F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.quarter("hire_date").alias("hire_date_quarter"),
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("job_title", F.lit("UNKNOWN")).alias("resolved_job_title"),
    )
)

enriched = (
    valid
    .withColumn("prev_hire_date_quarter", F.lag("hire_date_quarter").over(w))
    .withColumn("next_salary", F.lead("salary").over(w))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("running_hire_date_quarter", F.sum("hire_date_quarter").over(w2))
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

kept_keys = (
    latest
    .select("level", "dept_id")
    .subtract(
        df_employees
        .filter(F.col("dept_id").isin([3, 5, 11, 20]))
        .select("level", "dept_id")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "full_name",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "hire_date_quarter",
        "is_remote_flag",
        "resolved_job_title",
        F.when(F.col("level").rlike("^(L2|L4)$"), "PRIORITY").when(F.col("level").rlike("^(L1|L3)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("level_zone"),
    )
    .orderBy("level")
)
