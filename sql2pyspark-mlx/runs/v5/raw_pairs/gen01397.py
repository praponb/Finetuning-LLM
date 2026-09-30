from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Chiang Mai", "Bangkok"]))
    .select(
        "dept_name",
        "budget",
        "location",
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
    )
    .orderBy("dept_name_title")
)
