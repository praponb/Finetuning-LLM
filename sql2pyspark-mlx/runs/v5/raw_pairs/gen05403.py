from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.to_date("started_at") < "2023-10-20") | F.col("browser").isin(["Firefox", "Chrome"]))
    .select(
        "id",
        "browser",
        "customer_id",
        F.to_date("started_at").alias("started_at_date"),
        F.initcap(F.trim("country")).alias("country_title"),
    )
)
