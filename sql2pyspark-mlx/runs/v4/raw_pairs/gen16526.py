from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Dow%"))
    .select("region", "sqft")
    .unionByName(
        df_stores
        .filter(F.col("city").isin(["Singapore", "Berlin", "New York"]))
        .select("region", "sqft")
    )
    .distinct()
)
