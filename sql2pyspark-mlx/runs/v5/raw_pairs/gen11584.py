from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") <= 3600)
    .select("id", "sale_date")
    .unionByName(
        df_store_sales
        .filter(F.col("store_id").isin([3, 5]))
        .select("id", "sale_date")
    )
    .distinct()
)
