from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L4")
    .select(
        "manager_id",
        "salary",
        "level",
        F.when(F.col("level").isin(["L4", "L2"]), "Core").otherwise("Other").alias("level_group"),
    )
)
