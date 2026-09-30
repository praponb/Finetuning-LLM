from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-03-29", "2024-05-03") & F.col("product_id").isin([8, 9]))
    .select("id", "product_id")
    .orderBy("id")
)
