from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy(F.col("opened_date").asc_nulls_first(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("age").desc(), "id")
w3 = Window.partitionBy("account_type", "city")
w4 = Window.partitionBy("city", "account_type").orderBy("balance", "id")
w_running = Window.partitionBy("customer_id").orderBy("prev_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy("running_max_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w8 = Window.orderBy("running_min_age", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.orderBy("running_min_age", "id")

latest = (
    df_accounts
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

joined = (
    latest
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .filter(~F.col("p.country").isin(["SG", "US"]))
    .select("src.*", F.col("p.age"), F.col("p.city"), F.col("p.country"))
)

ranked = (
    joined
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .select(
        "*",
        F.sum("age").over(w3).alias("account_type_sum_age"),
        F.lag("age", 1).over(w4).alias("prev_age"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "opened_date",
        "status",
        "rank_in_group",
        "age",
        "city",
        "country",
        "latest_rank",
        "account_type_sum_age",
        "prev_age",
        F.sum("balance").over(w_running).alias("running_sum_balance"),
        F.avg("balance").over(w_running).alias("running_avg_balance"),
        F.max("balance").over(w_running).alias("running_max_balance"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.avg("age").over(w6).alias("running_avg_age"),
        F.min("age").over(w6).alias("running_min_age"),
        F.count("age").over(w6).alias("running_count_age"),
        F.sum("age").over(w6).alias("running_sum_age"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.rank_in_group",
        "s.latest_rank",
        "s.account_type_sum_age",
        "s.prev_age",
        "s.running_max_balance",
        "s.running_avg_age",
        "s.running_min_age",
        "s.running_sum_age",
        F.col("c.amount").alias("transaction_amount"),
    )
)

result = (
    with_latest
    .select(
        "id",
        "rank_in_group",
        "prev_age",
        "running_max_balance",
        "running_min_age",
        "running_sum_age",
        "transaction_amount",
        F.count("running_sum_age").over(w8).alias("last7_count_running_sum_age"),
        F.avg("running_sum_age").over(w8).alias("last7_avg_running_sum_age"),
        F.max("running_sum_age").over(w8).alias("last7_max_running_sum_age"),
        F.lead("running_avg_age", 2).over(w9).alias("next_running_avg_age"),
    )
)
