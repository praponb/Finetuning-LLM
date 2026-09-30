from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "level",
        F.substring("full_name", 1, 4).alias("full_name_prefix"),
    )
    .orderBy(F.col("id").desc())
)
