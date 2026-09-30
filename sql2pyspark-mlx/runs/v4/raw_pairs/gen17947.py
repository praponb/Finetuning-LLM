from pyspark.sql import functions as F

result = (
    df_stores
    .select("city", "region")
    .intersect(
        df_stores
        .filter(F.length("store_name") > 10)
        .select("city", "region")
    )
)
