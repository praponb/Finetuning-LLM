from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("transaction_txn_type").orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_last = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.merchant").alias("transaction_merchant"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

ranked = (
    with_last
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "status")
)

joined = (
    ranked
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select("src.id", "src.customer_id", F.col("c.age"), F.col("c.name"))
)

metrics = (
    joined
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "age",
        "name",
        F.min("age").over(w3).alias("running_min_age"),
        F.count("age").over(w3).alias("running_count_age"),
        F.avg("age").over(w3).alias("running_avg_age"),
    )
)

with_last_accounts = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.age",
        "s.running_min_age",
        "s.running_count_age",
        "s.running_avg_age",
        F.col("c.amount").alias("transaction_amount"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

result = (
    with_last_accounts
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["country", "transaction_txn_type"], ["country"], []],
        F.col("d.country").alias("country"),
        F.col("a.transaction_txn_type").alias("transaction_txn_type"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("a.running_min_age")).alias("avg_running_min_age"),
        F.grouping_id().alias("grouping_level"),
    )
)
