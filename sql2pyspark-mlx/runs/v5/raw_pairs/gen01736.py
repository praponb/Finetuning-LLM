from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 3052000) & ((F.col("location") == "London") | (F.col("location") == "Tokyo")))
    .select(
        "id",
        "location",
        F.trim("dept_name").alias("clean_dept_name"),
        F.col("budget").cast("int").alias("budget_int"),
    )
)
