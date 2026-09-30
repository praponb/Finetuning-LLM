from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "job_title").orderBy("hire_date", "id")
w2 = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc_nulls_last(), "id")
w3 = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("manager_id").orderBy("hire_date", "id")
w5 = Window.partitionBy("manager_id").orderBy(F.col("latest_rank").desc(), "id")

valid = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-11-05").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)

ranked = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .select(
        "*",
        F.first("job_title", ignorenulls=True).over(w3).alias("first_known_job_title"),
        F.row_number().over(w4).alias("seq_no"),
        F.row_number().over(w5).alias("row_seq"),
    )
)
