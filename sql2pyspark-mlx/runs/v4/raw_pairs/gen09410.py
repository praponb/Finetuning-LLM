from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") > 3852000) & (F.col("location") == "Bangkok"))
    .select(
        "location",
        "dept_name",
        "budget",
        F.initcap(F.trim("location")).alias("location_title"),
    )
)
