from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at", "id", "k")
w2 = Window.orderBy(F.col("created_at").desc(), "id", "k")
w3 = Window.partitionBy("k").orderBy("created_at", "id", "k")
w_hist = Window.partitionBy("customer_id", "k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("customer_id").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

lines = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("attributes").alias("k", "v"))
)

metrics = (
    lines
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("first_known_k", F.first("k", ignorenulls=True).over(w_hist))
    .withColumn("first_known_k_2", F.first("k", ignorenulls=True).over(w_grp))
)

enriched = (
    metrics
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.*", F.col("c.city"), F.col("c.email"))
)

valid = (
    enriched
    .filter(F.col("row_seq") != 8)
    .select(
        "id",
        "customer_id",
        "created_at",
        "k",
        "v",
        "seq_no",
        "row_seq",
        "first_known_k",
        "first_known_k_2",
        F.least("seq_no", "event_seq").alias("least_seq_no_event_seq"),
        F.coalesce("k", F.lit("OTHER")).alias("k_filled"),
    )
)

result = (
    valid
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "created_at",
        "k",
        "v",
        "row_seq",
        "first_known_k",
        "first_known_k_2",
        "least_seq_no_event_seq",
        F.when(F.col("k").rlike("^(gift|device)$"), "PRIORITY").when(F.col("k").rlike("^(utm_source|weight_kg)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("k_zone"),
        F.col("avg_account_balance"),
    )
    .orderBy("customer_id")
)
