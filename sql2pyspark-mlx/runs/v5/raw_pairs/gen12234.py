from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") >= "2024-09-01")
    .select("id", "region")
    .unionByName(
        df_stores
        .filter(F.col("region").isin(["North", "West", "East"]))
        .select("id", "region")
    )
    .distinct()
    .orderBy(F.col("region").desc())
)
