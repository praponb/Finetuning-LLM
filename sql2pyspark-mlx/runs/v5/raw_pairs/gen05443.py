from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull() & F.col("location").isin(["Bangkok", "Chiang Mai", "London"]))
    .select(
        "id",
        "location",
        F.concat_ws(",", "dept_name", "location").alias("dept_name_location_key"),
        F.ceil("budget").alias("budget_whole"),
    )
    .orderBy("id")
    .limit(5)
)
