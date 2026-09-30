from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

base = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-07-08").cast("date"))
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "is_active",
        "age",
        F.concat("country", F.lit(" "), "city").alias("country_city"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

result = (
    base
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.segment",
        "s.is_active",
        "s.country_city",
        "s.city_filled",
        F.col("c.country").alias("session_country"),
        F.col("c.device"),
    )
)
