from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        "page",
        F.when(F.col("event_type").isin(["search", "page_view"]), "tier_1").otherwise("other").alias("event_type_group"),
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
    )
    .orderBy("page", F.col("purchase_count").desc())
)
