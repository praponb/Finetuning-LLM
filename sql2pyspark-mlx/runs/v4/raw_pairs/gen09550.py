from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(9, 17))
    .select("id", "browser", "customer_id")
    .unionByName(
        df_sessions
        .filter(F.col("country") == "US")
        .select("id", "browser", "customer_id")
    )
    .distinct()
    .orderBy(F.col("id").desc())
)
