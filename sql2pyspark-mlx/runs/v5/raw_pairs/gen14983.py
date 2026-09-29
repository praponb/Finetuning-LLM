from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2]))
    .select("sale_date")
    .intersect(df_employees.select("hire_date"))
    .orderBy("sale_date")
)
