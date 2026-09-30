from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter(F.col("region").isNotNull() & ~F.col("store_name").like("%Centr%"))
    .select("*", F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"))
)

result = (
    filtered
    .select("region", "id")
    .subtract(
        df_stores
        .filter(F.col("store_name").like("R%"))
        .select("region", "id")
    )
)
