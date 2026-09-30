from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("adjusted_balance").desc(), "id")
w2 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("opened_date", "id")
w5 = Window.partitionBy("account_type").orderBy(F.col("row_seq").desc_nulls_first(), "id")
w6 = Window.partitionBy("account_type").orderBy("row_seq", "id")
w7 = Window.partitionBy("account_type").orderBy(F.col("row_seq").desc(), "id")
w8 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("status", "account_type")
w_hist = Window.orderBy(F.col("row_seq").desc_nulls_last())
w11 = Window.partitionBy("account_type").orderBy("row_seq_share", "id")
w12 = Window.partitionBy("status")
w_grp = Window.partitionBy("status").orderBy("row_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("row_seq_share", "id").rowsBetween(-6, -1)

base = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-03-25").cast("date"))
    .select(
        "*",
        F.add_months("opened_date", 3).alias("opened_date_plus_months"),
        F.round((F.col("balance") * 100), 2).alias("adjusted_balance"),
    )
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "account_type", "opened_date", "status")
)

with_windows = (
    deduped
    .select(
        "id",
        "account_type",
        "status",
        F.first("status", ignorenulls=True).over(w2).alias("first_known_status"),
        F.first("status", ignorenulls=True).over(w3).alias("first_known_status_2"),
        F.row_number().over(w4).alias("row_seq"),
        F.last("status", ignorenulls=True).over(w2).alias("last_known_status"),
    )
)

deduped_accounts = (
    with_windows
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    deduped_accounts
    .select(
        "id",
        "account_type",
        "status",
        "first_known_status",
        "first_known_status_2",
        "row_seq",
        "last_known_status",
        F.lead("row_seq", 2).over(w6).alias("next_row_seq"),
        F.row_number().over(w7).alias("row_seq_2"),
        F.sum("row_seq").over(w8).alias("running_row_seq"),
        F.round((F.col("row_seq") / F.sum("row_seq").over(w9)), 4).alias("row_seq_share"),
        F.rank().over(w_hist).alias("row_seq_rank"),
    )
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w11))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "status",
        "first_known_status",
        "row_seq",
        "row_seq_2",
        "row_seq_share",
        "row_seq_rank",
    )
)

with_windows_accounts = (
    latest
    .select(
        "id",
        "first_known_status",
        "row_seq",
        "row_seq_rank",
        F.round((F.col("row_seq") / F.sum("row_seq").over(w12)), 4).alias("row_seq_share_2"),
        F.last("row_seq_rank", ignorenulls=True).over(w_grp).alias("last_known_row_seq_rank"),
        F.avg("row_seq").over(w_recent).alias("row_seq_avg_6_prior"),
        F.round((F.col("row_seq_2") / F.sum("row_seq_2").over(w12)), 4).alias("row_seq_2_share"),
    )
)

result = (
    with_windows_accounts
    .withColumn("row_seq_2_share_tier", F.when(F.col("row_seq_2_share") >= 0.81, "PREMIUM").otherwise("STANDARD"))
    .withColumn("row_seq_tier", F.when(F.col("row_seq") >= 7, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["row_seq_2_share_tier", "row_seq_tier"], ["row_seq_2_share_tier"], []],
        "row_seq_2_share_tier",
        "row_seq_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.max("last_known_row_seq_rank").alias("max_last_known_row_seq_rank"),
    )
    .orderBy("avg_row_seq")
)
