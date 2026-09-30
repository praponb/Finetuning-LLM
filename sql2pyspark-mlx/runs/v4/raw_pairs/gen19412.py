from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country") != "US") & (F.col("browser") == "Edge") & F.col("customer_id").isin([4, 13]))
    .select(
        "id",
        "browser",
        "customer_id",
        F.initcap(F.trim("country")).alias("country_title"),
    )
)
