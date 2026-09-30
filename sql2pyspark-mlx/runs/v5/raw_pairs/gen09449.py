from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc_nulls_last(), "id")

deduped = (
    df_sessions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined = (
    deduped
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("c.country").isin(["US", "DE"]))
    .select(
        "e.id",
        "e.customer_id",
        "e.browser",
        "e.started_at",
        F.col("c.signup_date"),
        F.col("c.country").alias("customer_country"),
        F.col("c.segment"),
    )
)

matched_keys = (
    joined
    .select("customer_id", "browser")
    .intersect(
        df_sessions
        .filter(F.to_date("started_at") < "2024-04-01")
        .select("customer_id", "browser")
    )
)

kept_rows = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_anti")
)

result = (
    not_flagged
    .select("customer_id", "signup_date")
    .orderBy(F.col("signup_date").desc_nulls_last())
)
