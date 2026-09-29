from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@company.co"))
    .select("email")
    .intersect(df_employees.select("email"))
    .orderBy("email")
)
