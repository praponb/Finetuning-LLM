from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("full_name") > 10)
    .select(
        "id",
        "manager_id",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)
