from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("city").orderBy("balance", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("balance").desc(), "id")

filtered = (
    df_customers
    .filter(F.col("country").isin(["SG", "US"]))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.col("is_active").cast("int").alias("is_active_flag"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "country",
        "city",
        "segment",
        "is_active",
        "is_active_flag",
        "resolved_city",
        F.lag("is_active_flag").over(w).alias("prev_is_active_flag"),
        F.lead("credit_limit").over(w).alias("next_credit_limit"),
        F.lag("credit_limit").over(w).alias("prev_credit_limit"),
        F.row_number().over(w).alias("seq_no"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.segment",
        "s.resolved_city",
        "s.next_credit_limit",
        "s.prev_credit_limit",
        "s.seq_no",
        F.col("c.balance"),
    )
)

enriched = (
    with_latest
    .select(
        "*",
        F.min("balance").over(w_recent).alias("last7_min_balance"),
        F.avg("balance").over(w_recent).alias("last7_avg_balance"),
        F.max("balance").over(w_recent).alias("last7_max_balance"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

base = (
    enriched
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "country",
        "segment",
        "resolved_city",
        "next_credit_limit",
        "prev_credit_limit",
        "seq_no",
        "balance",
        "last7_avg_balance",
        "last7_max_balance",
        "row_seq",
        (F.col("row_seq") + F.col("seq_no")).alias("row_seq_plus_seq_no"),
        F.when(F.col("seq_no") >= 8, "High").when(F.col("seq_no") >= 2, "Medium").otherwise("Low").alias("seq_no_level"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_clean"),
    )
)

result = (
    base
    .select(
        "id",
        "country",
        "segment",
        "next_credit_limit",
        "prev_credit_limit",
        "seq_no",
        "row_seq",
        "row_seq_plus_seq_no",
    )
)
