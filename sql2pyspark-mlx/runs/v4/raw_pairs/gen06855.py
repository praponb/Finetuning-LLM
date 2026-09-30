from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2701000, 3211000))
    .select("id", "location", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("dept_name").isin(["Marketing", "Finance", "Engineering"]))
        .select("id", "location", "dept_name")
    )
)
