from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy("browser")
    .agg(
        F.min("started_at").alias("first_started_at"),
        F.countDistinct("country").alias("distinct_countries"),
    )
    .filter(F.col("distinct_countries") > 1)
    .orderBy(F.col("browser").desc())
)
