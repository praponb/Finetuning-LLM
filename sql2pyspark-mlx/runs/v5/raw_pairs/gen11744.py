from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["New York", "Bangkok"]))
    .select("location", "dept_name")
    .intersect(
        df_departments
        .filter(F.col("budget") < 2372000)
        .select("location", "dept_name")
    )
)
