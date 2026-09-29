from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Legal"]))
    .select("id", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("budget") > 1761000)
        .select("id", "dept_name")
    )
    .orderBy("dept_name")
)
