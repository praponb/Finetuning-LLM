from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("segment", "country").orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy("segment", "city").orderBy("segment_length", "id")
w_running = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")
w4 = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")
w5 = Window.partitionBy("city").orderBy("credit_limit_bucket", "id")
w6 = Window.partitionBy("city")

cleaned = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-06-01").cast("date"))
    .select("*", F.length("segment").alias("segment_length"))
)

enriched = (
    cleaned
    .select(
        "id",
        "city",
        "is_active",
        "credit_limit",
        "age",
        "segment_length",
        F.ntile(5).over(w_grp).alias("age_bucket"),
        F.lead("credit_limit", 1).over(w2).alias("next_credit_limit"),
        F.ntile(10).over(w_running).alias("age_bucket_2"),
        F.ntile(10).over(w4).alias("credit_limit_bucket"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.lag("age").over(w5).alias("prev_age"),
        F.lead("age").over(w5).alias("next_age"),
        F.lag("next_credit_limit").over(w5).alias("prev_next_credit_limit"),
        F.row_number().over(w5).alias("seq_no"),
        F.round((F.col("age") / F.sum("age").over(w6)), 4).alias("age_share"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.browser") != "Safari"), "left_semi")
)

kept_keys = (
    not_flagged
    .select("id", "city")
    .intersectAll(
        df_customers
        .filter(F.col("segment").isin(["Corporate", "Consumer", "SMB"]))
        .select("id", "city")
    )
)

verified = (
    not_flagged
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "*",
        F.round((F.col("seq_no") / F.sum("seq_no").over(w6)), 4).alias("seq_no_share"),
    )
)

result = (
    with_windows
    .select(
        "city",
        "is_active",
        "segment_length",
        "age_bucket",
        "next_credit_limit",
        "credit_limit_bucket",
        "prev_age",
        "seq_no",
        "seq_no_share",
    )
    .orderBy(F.col("seq_no").desc_nulls_last(), "segment_length")
)
