from pyspark.sql import functions as F

result = (
    df_departments
    .filter(((F.col("location") == "London") & F.col("location").isin(["Bangkok", "Chiang Mai"])) | (F.col("budget") <= 3857000))
    .select(
        "id",
        "location",
        F.concat("dept_name", F.lit(" "), "location").alias("dept_name_location"),
    )
)
