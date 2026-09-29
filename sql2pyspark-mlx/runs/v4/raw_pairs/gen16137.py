from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("store_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("units") <= 21)
        .select("store_id", "id")
    )
)
