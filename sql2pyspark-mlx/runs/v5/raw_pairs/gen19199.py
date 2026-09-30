from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "duration_sec",
        "page",
        F.when(F.col("page") == "/checkout", 1).otherwise(0).alias("is_checkout"),
    )
    .orderBy("id")
)
