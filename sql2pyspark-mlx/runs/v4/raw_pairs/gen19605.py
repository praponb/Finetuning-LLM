from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

cleaned = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2024-03-06").cast("date")) & F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        F.round("age", 0).alias("age_rounded"),
        F.concat("email", F.lit(" - "), "segment").alias("email_segment"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_clean"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.device").alias("session_device"))
)
