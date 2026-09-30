from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id").orderBy("row_num", "id")
w3 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status")
w5 = Window.orderBy("row_num", "id")
w6 = Window.partitionBy("account_type").orderBy(F.col("account_type_length").desc_nulls_last())
w7 = Window.orderBy(F.col("row_seq").desc_nulls_first())
w8 = Window.partitionBy("status").orderBy("status_avg_account_type_length", "id")
w_running = Window.partitionBy("status").orderBy(F.col("opened_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

filtered = (
    df_accounts
    .filter((F.col("opened_date") >= F.to_date(F.lit("2024-03-09"))) & F.col("customer_id").isin([1, 3, 7]))
    .withColumn("opened_date_period", F.date_trunc("YEAR", "opened_date"))
    .withColumn("account_type_length", F.length("account_type"))
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    ranked
    .select(
        "*",
        F.lead("balance", 1).over(w2).alias("next_balance"),
        F.first("balance", ignorenulls=True).over(w3).alias("first_known_balance"),
        F.avg("account_type_length").over(w4).alias("status_avg_account_type_length"),
        F.row_number().over(w5).alias("row_seq"),
        F.cume_dist().over(w6).alias("account_type_length_cume_dist"),
    )
)

with_windows = (
    enriched
    .withColumn("row_seq_dense_rank", F.dense_rank().over(w7))
    .withColumn("status_avg_account_type_length_bucket", F.ntile(10).over(w8))
    .withColumn("account_type_length_cume_dist_max_30d", F.max("account_type_length_cume_dist").over(w_running))
    .withColumn("status_count_first_known_balance", F.count("first_known_balance").over(w4))
)

result = (
    with_windows
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "opened_date_period",
        "row_num",
        "next_balance",
        "first_known_balance",
        "status_avg_account_type_length",
        F.col("max_subscription_monthly_fee"),
    )
    .orderBy(F.col("customer_id").desc_nulls_first())
)
