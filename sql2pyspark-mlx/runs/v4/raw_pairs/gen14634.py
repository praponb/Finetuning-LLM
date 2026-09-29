from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Chiang Mai", "Tokyo", "Bangkok"]))
    .select("id", "dept_name", "location")
    .subtract(
        df_departments
        .filter(~F.col("location").isin(["Chiang Mai", "Singapore"]))
        .select("id", "dept_name", "location")
    )
)
