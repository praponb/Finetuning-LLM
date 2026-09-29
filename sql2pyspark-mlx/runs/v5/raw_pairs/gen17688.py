from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1250000, 2719000))
    .groupBy("dept_name")
    .pivot("location", ["Chiang Mai", "New York", "London", "Bangkok"])
    .agg(F.count("budget"))
)
