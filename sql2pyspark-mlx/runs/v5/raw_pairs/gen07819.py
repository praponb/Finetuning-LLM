from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") <= 120000)
    .select("hire_date")
    .union(df_shipments.select("shipped_date"))
    .orderBy("hire_date")
)
