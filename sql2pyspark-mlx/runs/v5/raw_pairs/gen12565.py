from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id")
w2 = Window.partitionBy("status", "customer_id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("next_balance").desc(), "id")
w4 = Window.orderBy(F.col("status_avg_balance").asc_nulls_last())
w5 = Window.partitionBy("customer_id")

with_windows = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "status",
        F.lead("balance", 1).over(w).alias("next_balance"),
        F.avg("balance").over(w2).alias("status_avg_balance"),
    )
)

latest = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

remaining_keys = (
    latest
    .select("id")
    .subtract(
        df_accounts
        .filter(F.col("opened_date") > "2023-08-01")
        .select("id")
    )
)

verified = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_keys = (
    verified
    .select("status")
    .intersect(
        df_accounts
        .filter(F.month("opened_date").isin([1, 3, 10]))
        .select("status")
    )
)

kept_rows = (
    verified
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("status_avg_balance_percent_rank", F.percent_rank().over(w4))
    .withColumn("customer_id_max_status_avg_balance", F.max("status_avg_balance").over(w5))
)

joined = (
    enriched
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.city") == "Singapore")
    .select("s.*", F.col("p.credit_limit"), F.col("p.segment"), F.col("p.age"))
)

result = (
    joined
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.avg("status_avg_balance_percent_rank").alias("avg_status_avg_balance_percent_rank"),
        F.sort_array(F.collect_set("status")).alias("status_list_2"),
        F.max(F.when(F.col("status") == "CLOSED", F.col("status_avg_balance_percent_rank"))).alias("closed_max_status_avg_balance_percent_rank"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("closed_max_status_avg_balance_percent_rank").desc())
)
