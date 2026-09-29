from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("end_date_year").desc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id")
w3 = Window.partitionBy("status", "customer_id").orderBy("end_date_year", "id")
w4 = Window.partitionBy("plan").orderBy("latest_rank", "id").rowsBetween(-5, -1)
w_part = Window.orderBy(F.col("end_date_year").desc(), "id")
w_ordered = Window.partitionBy("status", "plan")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

base = (
    df_subscriptions
    .filter((F.col("end_date") > F.to_date(F.lit("2023-08-14"))) & F.col("customer_id").isNotNull() & (F.col("end_date") < "2025-08-03"))
    .select(
        "*",
        F.date_format("start_date", "yyyy-MM-dd").alias("start_date_label"),
        F.year("end_date").alias("end_date_year"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

base_subscriptions = (
    latest
    .filter(F.col("customer_id").isNotNull() & (F.col("latest_rank") >= 1))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "start_date_label",
        "end_date_year",
        "rank_in_group",
        "latest_rank",
        F.least("end_date_year", "rank_in_group").alias("least_end_date_year_rank_in_group"),
    )
)

enriched = (
    base_subscriptions
    .select(
        "*",
        F.ntile(3).over(w3).alias("end_date_year_bucket"),
        F.avg("end_date_year").over(w4).alias("end_date_year_avg_5_prior"),
        F.ntile(10).over(w_part).alias("end_date_year_bucket_2"),
        F.sum("end_date_year").over(w_ordered).alias("status_sum_end_date_year"),
    )
)

matched_keys = (
    enriched
    .select("plan", "status")
    .exceptAll(
        df_subscriptions
        .filter(~F.col("plan").isin(["enterprise", "standard"]))
        .select("plan", "status")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        "status",
        "start_date_label",
        "end_date_year",
        "rank_in_group",
        "latest_rank",
        F.col("min_account_balance"),
    )
    .orderBy(F.col("latest_rank").desc())
)
