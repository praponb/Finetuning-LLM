from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("id", "store_id")
    .intersect(
        df_store_sales
        .filter(F.col("sale_date") >= "2025-02-01")
        .select("id", "store_id")
    )
)
