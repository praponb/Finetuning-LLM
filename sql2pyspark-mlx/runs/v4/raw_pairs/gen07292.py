from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") > 3600)
    .select("id", "units", "store_id")
    .unionByName(
        df_store_sales
        .filter(F.col("store_id").isin([1, 2, 4]))
        .select("id", "units", "store_id")
    )
    .distinct()
)
