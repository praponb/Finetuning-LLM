from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.year("sale_date") == 2025)
    .select("id", "product_id", "sale_date")
    .unionByName(
        df_store_sales
        .filter(F.col("product_id").isin([8, 9, 10]))
        .select("id", "product_id", "sale_date")
    )
    .distinct()
)
