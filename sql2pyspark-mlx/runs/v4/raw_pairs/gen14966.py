from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isin([3, 8]))
    .select("browser", "country")
    .unionByName(
        df_sessions
        .filter(F.col("browser") != "Firefox")
        .select("browser", "country")
    )
    .distinct()
    .orderBy(F.col("browser").desc())
)
