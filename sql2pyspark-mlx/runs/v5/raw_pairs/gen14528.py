from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "salary",
        F.upper(F.trim("full_name")).alias("full_name_normalized"),
        F.length("full_name").alias("full_name_length"),
    )
)
