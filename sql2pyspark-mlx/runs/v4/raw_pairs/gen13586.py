from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Dow%"))
    .select("region")
    .unionByName(
        df_stores
        .filter(F.lower(F.trim("store_name")) == "siam flagship")
        .select("region")
    )
)
