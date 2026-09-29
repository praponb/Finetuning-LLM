from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-08-01", "2024-07-28"))
    .select("revenue")
    .union(df_payments.select("amount"))
    .orderBy(F.col("revenue").desc())
)
