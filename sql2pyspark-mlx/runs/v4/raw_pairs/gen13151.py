from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNotNull())
    .select("opened_date", "store_name")
    .subtract(
        df_stores
        .filter(F.col("opened_date") >= "2025-02-11")
        .select("opened_date", "store_name")
    )
)
