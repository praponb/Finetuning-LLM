from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-07-07").cast("date")) & F.col("status").isNotNull())
    .withColumn("opened_date_label", F.date_format("opened_date", "yyyy-MM"))
    .withColumn("status_filled", F.coalesce("status", F.lit("DIRECT")))
)

base_accounts = (
    base
    .filter(F.col("status").isNotNull() & (~F.col("status").eqNullSafe(F.lit("FROZEN"))))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "opened_date_label",
        "status_filled",
        F.concat("status", F.lit(", "), "account_type").alias("status_account_type"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_clean"),
    )
)

enriched = (
    base_accounts
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.status_filled",
        F.col("c.country"),
        F.col("c.segment"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select("a.id", "a.customer_id", "a.segment", F.col("p.city"), F.col("p.age"))
)

with_latest = (
    with_dims
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time"))
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.merchant") != "7-Eleven"), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "segment",
        "age",
        "txn_time",
        F.when(F.col("segment").rlike("^(Corporate|SMB)$"), "GROUP_B").when(F.col("segment").rlike("^(Consumer)$"), "EXTENDED").otherwise("CORE").alias("segment_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
