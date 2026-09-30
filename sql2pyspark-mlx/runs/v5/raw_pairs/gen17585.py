from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["New York", "Singapore", "Bangkok"]))
    .select("dept_name", "location")
    .distinct()
)
