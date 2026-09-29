from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city")
    .subtract(df_departments.select("location"))
)
