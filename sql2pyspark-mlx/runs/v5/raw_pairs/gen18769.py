from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .select(
        "id",
        "dept_id",
        F.trim("full_name").alias("clean_full_name"),
        F.when(F.col("level").isin(["L3", "L2"]), "Core").otherwise("Other").alias("level_group"),
    )
)
