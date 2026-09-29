from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "manager_id",
        "is_remote",
        F.upper(F.trim("full_name")).alias("full_name_normalized"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)
