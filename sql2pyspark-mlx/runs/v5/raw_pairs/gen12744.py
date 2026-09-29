from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") > 89000)
    .select(
        "id",
        "level",
        "full_name",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
    .orderBy(F.col("full_name").desc(), F.col("is_remote_label").desc())
)
