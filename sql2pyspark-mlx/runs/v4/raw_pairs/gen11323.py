from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 4, 6]))
    .select("sale_date")
    .unionByName(
        df_store_sales
        .filter(F.col("sale_date") <= "2024-10-01")
        .select("sale_date")
    )
    .distinct()
)
