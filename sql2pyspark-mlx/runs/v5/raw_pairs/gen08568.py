from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("segment").isin(["Corporate", "SMB"]))
    .select("email")
    .subtract(df_employees.select("email"))
    .orderBy(F.col("email").desc())
)
