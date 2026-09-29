from pyspark.sql import functions as F

result = (
    df_stores
    .select("region", "id")
    .intersectAll(
        df_stores
        .filter(F.col("opened_date") < "2025-06-03")
        .select("region", "id")
    )
)
