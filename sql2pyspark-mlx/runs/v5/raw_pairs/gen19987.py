from pyspark.sql import functions as F

result = (
    df_stores
    .select("id")
    .intersect(
        df_stores
        .filter(F.col("region") != "East")
        .select("id")
    )
)
