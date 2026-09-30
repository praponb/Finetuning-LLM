from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(3400, 4000))
    .select("store_id", "units")
    .intersect(
        df_store_sales
        .filter(F.col("revenue").between(2000, 3400))
        .select("store_id", "units")
    )
)
