from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L4", "L3"]))
    .select(
        "level",
        F.upper("level").alias("level_upper"),
        F.substring("full_name", 1, 2).alias("full_name_prefix"),
    )
)
