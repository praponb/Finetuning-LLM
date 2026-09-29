from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 2190000)
    .select("location")
    .subtract(df_customers.filter(F.col("name").like("Ja%")).select("city"))
)
