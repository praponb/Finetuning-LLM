from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 1700)
    .select("sale_date", "product_id")
    .unionByName(
        df_store_sales
        .filter(F.year("sale_date") == 2023)
        .select("sale_date", "product_id")
    )
)
