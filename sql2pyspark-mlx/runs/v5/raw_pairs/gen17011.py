from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").asc_nulls_first())
w2 = Window.partitionBy("plan", "customer_id").orderBy("end_date_plus_months", "id")
w_hist = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("monthly_fee_avg_2", "id")
w7 = Window.partitionBy("status").orderBy("running_monthly_fee_2", "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

filtered = (
    df_subscriptions
    .filter(F.col("status").isin(["EXPIRED", "ACTIVE"]))
    .withColumn("end_date_plus_months", F.add_months("end_date", 1))
)

ranked = (
    filtered
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    ranked
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("monthly_fee_avg_2", F.avg("monthly_fee").over(w_hist))
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w4))
    .withColumn("running_monthly_fee_2", F.sum("monthly_fee").over(w5))
)

metrics = (
    enriched
    .filter(F.col("row_seq") > 5)
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "status",
        "row_seq",
        "running_monthly_fee",
        F.lag("running_monthly_fee").over(w6).alias("prev_running_monthly_fee"),
        F.lag("monthly_fee").over(w6).alias("prev_monthly_fee"),
        F.lead("monthly_fee").over(w6).alias("next_monthly_fee"),
        F.row_number().over(w6).alias("seq_no"),
        F.ntile(3).over(w7).alias("running_monthly_fee_2_bucket"),
    )
)

result = (
    metrics
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        "row_seq",
        "running_monthly_fee",
        "prev_running_monthly_fee",
        "prev_monthly_fee",
        "next_monthly_fee",
        F.col("min_account_balance"),
    )
    .orderBy(F.col("prev_running_monthly_fee").asc_nulls_last(), F.col("id").desc())
)
