from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Engineering") & ~F.col("location").isin(["Tokyo", "Bangkok"]) & (F.col("location") != "London"))
    .select(
        "id",
        "location",
        F.when(F.col("location") == "London", 1).otherwise(0).alias("is_london"),
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
    )
)
