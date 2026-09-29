from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("age").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

with_dims = (
    df_accounts
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select("e.*", F.col("c.credit_limit"), F.col("c.segment"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.opened_date",
        "e.status",
        "e.credit_limit",
        F.col("p.segment").alias("customer_segment"),
        F.col("p.name"),
        F.col("p.age"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

filtered = (
    deduped
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-07-13"))) & F.col("customer_segment").isNotNull())
    .select(
        "id",
        "customer_id",
        "opened_date",
        "status",
        "credit_limit",
        "customer_segment",
        "age",
        "rank_in_group",
        (F.col("credit_limit") + F.col("rank_in_group")).alias("credit_limit_plus_rank_in_group"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.status",
        "s.credit_limit",
        "s.customer_segment",
        "s.age",
        "s.rank_in_group",
        F.col("c.amount"),
    )
)

result = (
    with_last
    .groupBy(
        F.when(F.col("credit_limit") >= 14100, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
    )
    .agg(
        F.countDistinct("customer_segment").alias("distinct_customer_segments"),
        F.sort_array(F.collect_set("customer_segment")).alias("customer_segment_list"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
    .orderBy("distinct_customer_segments")
)
