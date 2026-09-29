from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date_plus_30d").desc(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("signup_date_plus_30d").desc())

cleaned = (
    df_customers
    .filter(F.col("city").isNotNull() & F.col("segment").isin(["Consumer", "SMB", "Corporate"]))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        F.date_add("signup_date", 30).alias("signup_date_plus_30d"),
        F.concat("country", F.lit(" "), "email").alias("country_email"),
    )
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

latest_customers = (
    latest
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

eligible = (
    latest_customers
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.country").isNull()), "left_semi")
)

result = (
    eligible
    .select("id", "name", "country", "city", "segment", "signup_date", "is_active")
    .orderBy(F.col("segment").desc_nulls_first())
)
