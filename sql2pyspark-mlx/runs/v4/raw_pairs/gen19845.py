from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 2545000)
    .select("id", "location", "dept_name")
    .subtract(
        df_departments
        .filter(~F.col("location").isin(["Tokyo", "Singapore"]))
        .select("id", "location", "dept_name")
    )
    .orderBy("location")
)
