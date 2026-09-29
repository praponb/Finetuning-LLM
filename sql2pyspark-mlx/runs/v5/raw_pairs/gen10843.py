from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Firefox"]))
    .select("id", "customer_id", "browser")
    .unionByName(
        df_sessions
        .filter(F.col("customer_id").isNotNull())
        .select("id", "customer_id", "browser")
    )
    .distinct()
    .orderBy("browser")
)
