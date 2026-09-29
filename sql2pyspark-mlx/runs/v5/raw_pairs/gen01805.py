from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("segment").orderBy(F.col("account_balance").desc(), "id")
w3 = Window.partitionBy("country").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("country").orderBy(F.col("order_date").desc_nulls_last(), "id")
w7 = Window.partitionBy("order_status", "country").orderBy(F.col("credit_limit").desc_nulls_last())

with_last = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.country",
        "s.city",
        "s.segment",
        "s.is_active",
        "s.credit_limit",
        F.col("c.status").alias("account_status"),
        F.col("c.balance").alias("account_balance"),
    )
)

latest = (
    with_last
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    latest
    .select(
        "id",
        "country",
        "segment",
        "credit_limit",
        "account_status",
        "account_balance",
        "latest_rank",
        F.sum("account_balance").over(w3).alias("running_account_balance"),
        F.percent_rank().over(w4).alias("credit_limit_percent_rank"),
    )
)

with_last_customers = (
    scored
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("order_status"), F.col("c.order_date"))
)

ranked = (
    with_last_customers
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

deduped = (
    ranked
    .withColumn("row_num", F.rank().over(w7))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("MONTH", "order_date").alias("activity_month"))
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.size(F.collect_set("order_status")).alias("num_order_status"),
        F.round(F.avg("credit_limit_percent_rank"), 2).alias("avg_credit_limit_percent_rank"),
        F.sum("credit_limit").alias("total_credit_limit"),
        F.max(F.when(F.col("country") == "UK", F.col("running_account_balance"))).alias("uk_max_running_account_balance"),
        F.size(F.collect_set("account_status")).alias("num_account_status"),
    )
    .orderBy("max_credit_limit")
)
