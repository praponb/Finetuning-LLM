from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") >= 37)
    .select("store_id", "sale_date")
    .intersect(
        df_store_sales
        .filter(F.col("store_id").isin([1, 2, 3]))
        .select("store_id", "sale_date")
    )
)
