from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 3533000) & ~F.col("location").isin(["Tokyo", "Chiang Mai", "New York"]))
    .select("id", "dept_name")
)
