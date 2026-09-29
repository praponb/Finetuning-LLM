from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "North")
    .select("id", "region")
    .unionByName(
        df_stores
        .filter(F.col("store_name").like("Ri%"))
        .select("id", "region")
    )
    .distinct()
)
