from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy("end_date", "id")
w2 = Window.partitionBy("plan").orderBy("rank_in_group", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("customer_id", "status").orderBy("rank_in_group", "id")
w4 = Window.partitionBy("status", "plan")
w5 = Window.partitionBy("status").orderBy("monthly_fee", "id")
w6 = Window.partitionBy("status")
w7 = Window.orderBy("monthly_fee_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status", "customer_id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

deduped = (
    df_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    deduped
    .select(
        "*",
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_2_prior"),
        F.lead("monthly_fee", 2).over(w3).alias("next_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w4)), 4).alias("monthly_fee_share"),
    )
)

enriched = (
    with_windows
    .withColumn("prev_monthly_fee_share", F.lag("monthly_fee_share", 1).over(w5))
    .withColumn("status_max_monthly_fee", F.max("monthly_fee").over(w6))
    .withColumn("first_known_monthly_fee_avg_2_prior", F.first("monthly_fee_avg_2_prior", ignorenulls=True).over(w7))
    .withColumn("monthly_fee_share_2", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w8)), 4))
)

result = (
    enriched
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "rank_in_group",
        "monthly_fee_avg_2_prior",
        "next_monthly_fee",
        "prev_monthly_fee_share",
        F.col("min_account_balance"),
    )
    .orderBy(F.col("monthly_fee_avg_2_prior").desc_nulls_last())
)
