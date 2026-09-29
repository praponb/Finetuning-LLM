from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy(F.col("age").desc(), "id")

cleaned = (
    df_customers
    .filter(~F.col("is_active"))
    .select(
        "*",
        F.when(F.col("country").isin(["US", "SG"]), "Core").otherwise("Other").alias("country_group"),
        F.upper("city").alias("city_upper"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

valid = (
    cleaned
    .filter(F.col("segment").isNotNull() & (F.col("segment") != "SMB"))
    .select(
        "id",
        "name",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "country_group",
        "city_upper",
        F.upper("country").alias("country_upper"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") >= F.col("s.signup_date")), "left_semi")
)

deduped = (
    eligible
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "name", "segment", "signup_date", "city_upper")
)

result = deduped.select("id", "name", "city_upper")
