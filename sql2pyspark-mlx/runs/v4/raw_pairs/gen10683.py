from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([3, 5, 7]))
    .select("city", "store_name")
    .unionByName(
        df_stores
        .filter(F.col("store_name").like("Si%"))
        .select("city", "store_name")
    )
)
