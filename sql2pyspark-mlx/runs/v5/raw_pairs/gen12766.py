from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("city").isin(["Chiang Mai", "London", "Tokyo"]))
    .select(F.col("city").alias("label"))
    .subtract(
        df_departments
        .filter(F.col("location").isin(["Berlin", "Tokyo", "London"]))
        .select(F.col("location").alias("label"))
    )
)
