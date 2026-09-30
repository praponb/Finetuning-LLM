from pyspark.sql import functions as F

result = (
    df_stores
    .select("region", "city")
    .intersect(
        df_stores
        .filter(F.month("opened_date").isin([1, 4, 8]))
        .select("region", "city")
    )
)
