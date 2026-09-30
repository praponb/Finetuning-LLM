from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Chiang Mai", "Tokyo"]) & (F.col("location") != "Chiang Mai"))
    .select(
        "budget",
        "dept_name",
        "location",
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
    )
    .orderBy(F.col("budget").desc())
)
