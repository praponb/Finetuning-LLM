from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") != "Bangkok") & (F.col("location") == "Chiang Mai") & (F.col("budget") <= 3393000))
    .select(
        "id",
        "location",
        "budget",
        F.concat("dept_name", F.lit("/"), "location").alias("dept_name_location"),
    )
)
