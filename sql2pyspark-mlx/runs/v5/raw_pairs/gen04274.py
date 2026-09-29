from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") > 2900)
    .select("store_id")
    .subtract(
        df_store_sales
        .filter(F.col("sale_date") <= "2024-08-01")
        .select("store_id")
    )
)
