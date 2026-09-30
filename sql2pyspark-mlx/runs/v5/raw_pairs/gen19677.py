from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "browser",
        F.initcap(F.trim("browser")).alias("browser_title"),
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
    )
    .orderBy("browser_title", "id")
    .limit(3)
)
