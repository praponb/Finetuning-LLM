from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "budget",
        "dept_name",
        "location",
        F.when(F.col("location") == "Chiang Mai", 1).otherwise(0).alias("is_chiang_mai"),
    )
)
