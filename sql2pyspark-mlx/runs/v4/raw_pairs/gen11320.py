from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc_nulls_last())
w2 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_accounts
    .filter(F.col("status").isNotNull() & F.col("customer_id").isin([3, 27, 28]))
    .withColumn("balance_level", F.when(F.col("balance") >= 66000, "High").when(F.col("balance") >= 27000, "Medium").otherwise("Low"))
)

joined = (
    base
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.name"))
)

deduped = (
    joined
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

joined_accounts = (
    deduped
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select(
        "e.*",
        F.col("d.name").alias("customer_name"),
        F.col("d.segment"),
        F.col("d.city"),
    )
)

kept = (
    joined_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_anti")
)

metrics = (
    kept
    .select(
        "*",
        F.last("balance", ignorenulls=True).over(w2).alias("last_known_balance"),
        F.sum("balance").over(w3).alias("running_balance"),
        F.first("segment", ignorenulls=True).over(w3).alias("first_known_segment"),
    )
)

cleaned = (
    metrics
    .filter((F.col("opened_date") > F.to_date(F.lit("2024-04-17"))) & F.col("customer_id").isNotNull() & F.col("customer_name").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "name",
        "customer_name",
        "segment",
        "last_known_balance",
        "running_balance",
        "first_known_segment",
        (F.col("running_balance") - F.col("balance")).alias("running_balance_minus_balance"),
        F.trim("segment").alias("clean_segment"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "name",
        "customer_name",
        "last_known_balance",
        "running_balance",
    )
    .orderBy(F.col("account_type").desc(), "last_known_balance")
)
