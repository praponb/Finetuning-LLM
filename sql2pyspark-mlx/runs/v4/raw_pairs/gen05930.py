from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([2, 6, 10]))
    .select("product_id")
    .unionByName(
        df_store_sales
        .filter(F.col("sale_date") <= "2025-03-22")
        .select("product_id")
    )
    .distinct()
    .orderBy("product_id")
)
