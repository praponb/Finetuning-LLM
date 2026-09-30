from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("age", "id")
w2 = Window.orderBy(F.col("age").desc(), "id")

filtered = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        F.lower("country").alias("country_lower"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_clean"),
    )
)

enriched = (
    filtered
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.id", "e.customer_id", "e.device", "e.country_lower", F.col("c.segment"))
)

kept = (
    enriched
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.to_date(F.col("x.event_time")) < "2025-03-16"), "left_anti")
)

joined = (
    kept
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.email").isNotNull())
    .select("s.id", "s.customer_id", "s.segment", F.col("p.age"), F.col("p.email"))
)

enriched_sessions = (
    joined
    .select(
        "id",
        "customer_id",
        "segment",
        "age",
        F.ntile(3).over(w).alias("age_bucket"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

summary = (
    enriched_sessions
    .groupBy(
        F.when(F.col("age_bucket") >= 8, "LARGE").otherwise("SMALL").alias("age_bucket_tier"),
        "segment",
    )
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_rows"),
        F.avg("age").alias("avg_age"),
        F.max(F.when(F.col("segment") == "SMB", F.col("age"))).alias("smb_max_age"),
    )
    .filter(F.col("corporate_rows") >= 1)
)

result = summary.select("age_bucket_tier", "segment", "avg_age", "smb_max_age")
