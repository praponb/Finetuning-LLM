from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy(F.col("age").desc())
w_recent = Window.partitionBy("device")
w3 = Window.partitionBy("device").orderBy(F.col("age").desc_nulls_first())
w_part = Window.orderBy(F.col("age").asc_nulls_first())
w_hist_2 = Window.partitionBy("city")

valid = (
    df_sessions
    .filter((~F.col("country").eqNullSafe(F.lit("US"))) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.when(F.col("browser").isin(["Safari", "Edge"]), "Core").otherwise("Other").alias("browser_group"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .filter(F.col("d.country") == "DE")
    .select("src.*", F.col("d.email"), F.col("d.segment"))
)

enriched_sessions = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.device",
        "a.country",
        "a.browser_group",
        "a.email",
        F.col("p.signup_date"),
        F.col("p.age"),
        F.col("p.city"),
    )
)

kept_keys = (
    enriched_sessions
    .select("device", "country")
    .intersect(
        df_sessions
        .filter(F.hour("started_at").between(6, 14))
        .select("device", "country")
    )
)

verified = (
    enriched_sessions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

enriched_sessions_2 = (
    verified
    .withColumn("age_cume_dist", F.cume_dist().over(w_hist))
    .withColumn("device_min_age", F.min("age").over(w_recent))
    .withColumn("age_rank", F.rank().over(w3))
    .withColumn("age_cume_dist_2", F.cume_dist().over(w_part))
    .withColumn("city_count_age", F.count("age").over(w_hist_2))
)

result = (
    enriched_sessions_2
    .alias("src")
    .join(df_customers.alias("dim"), F.col("src.customer_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["customer_country", "session_country"], ["customer_country"], []],
        F.col("dim.country").alias("customer_country"),
        F.col("src.country").alias("session_country"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("src.device_min_age")).alias("min_device_min_age"),
        F.round(F.sum(F.col("src.city_count_age")), 2).alias("total_city_count_age"),
    )
    .orderBy(F.col("min_device_min_age").desc_nulls_last())
)
