from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(2300, 2600))
    .select("id", "units", "sale_date")
)
