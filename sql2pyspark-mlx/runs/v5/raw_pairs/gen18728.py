from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 9)
    .select("id", "product_id", "sale_date")
    .unionByName(
        df_store_sales
        .filter(F.col("units") >= 20)
        .select("id", "product_id", "sale_date")
    )
    .distinct()
)
