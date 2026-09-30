from pyspark.sql import functions as F

result = (
    df_departments
    .filter(((F.col("location") != "Tokyo") & (F.col("budget") < 2064000)) | (F.col("budget") < 2795000))
    .select(
        "id",
        "location",
        "budget",
        "dept_name",
        F.trim("location").alias("clean_location"),
        F.initcap(F.trim("location")).alias("location_title"),
    )
)
