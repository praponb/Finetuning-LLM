from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date")
    .unionByName(
        df_shipments
        .filter(F.col("shipped_date") >= "2025-01-01")
        .select(F.col("shipped_date").alias("opened_date"))
    )
    .distinct()
    .orderBy("opened_date")
)
