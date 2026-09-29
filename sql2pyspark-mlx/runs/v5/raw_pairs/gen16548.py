from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([14, 15]))
    .select(
        "id",
        "manager_id",
        "level",
        "salary",
        F.upper(F.trim("full_name")).alias("full_name_normalized"),
    )
)
