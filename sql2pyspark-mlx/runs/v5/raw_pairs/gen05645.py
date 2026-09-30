from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc_nulls_last())
w_recent = Window.partitionBy("customer_id").orderBy("rn", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("start_date", "id")
w6 = Window.orderBy(F.col("last4_max_row_seq").desc())
w7 = Window.partitionBy("plan", "status").orderBy(F.col("start_date").desc(), "id")

enriched = (
    df_subscriptions
    .withColumn("customer_id_avg_monthly_fee", F.avg("monthly_fee").over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)

ranked = (
    enriched
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.max("row_seq").over(w_recent).alias("last4_max_row_seq"),
        F.min("row_seq").over(w_recent).alias("last4_min_row_seq"),
        F.avg("row_seq").over(w_recent).alias("last4_avg_row_seq"),
    )
)

enriched_subscriptions = (
    scored
    .filter(F.month("start_date").isin([7, 10, 11]))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "customer_id_avg_monthly_fee",
        "rn",
        "last4_min_row_seq",
        F.lag("customer_id_avg_monthly_fee", 1).over(w5).alias("prev_customer_id_avg_monthly_fee"),
        F.percent_rank().over(w6).alias("last4_max_row_seq_percent_rank"),
    )
)

latest = (
    enriched_subscriptions
    .withColumn("rn_2", F.row_number().over(w7))
    .filter(F.col("rn_2") == 1)
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "last4_min_row_seq",
        "prev_customer_id_avg_monthly_fee",
        "last4_max_row_seq_percent_rank",
    )
)

base = (
    latest
    .filter(F.col("customer_id") == 40)
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "last4_max_row_seq_percent_rank",
        F.floor("last4_max_row_seq_percent_rank").alias("last4_max_row_seq_percent_rank_whole"),
        (F.col("prev_customer_id_avg_monthly_fee") - F.col("last4_min_row_seq")).alias("prev_customer_id_avg_monthly_fee_minus_last4_min_row_seq"),
    )
)

filtered = (
    base
    .filter(F.col("prev_customer_id_avg_monthly_fee_minus_last4_min_row_seq") <= 28)
    .select(
        "*",
        (F.col("monthly_fee") + F.col("prev_customer_id_avg_monthly_fee_minus_last4_min_row_seq")).alias("monthly_fee_plus_prev_customer_id_avg_monthly_fee_minus_last4_min_row_seq"),
    )
)

result = (
    filtered
    .select(
        "id",
        F.col("monthly_fee_plus_prev_customer_id_avg_monthly_fee_minus_last4_min_row_seq").alias("MONTHLY_FEE_PLUS_PREV_CUSTOMER_ID_AVG_MONTHLY_FEE_MINUS_LAST4_MIN_ROW_SEQ"),
        F.col("last4_max_row_seq_percent_rank").alias("LAST4_MAX_ROW_SEQ_PERCENT_RANK_VAL"),
        F.col("prev_customer_id_avg_monthly_fee_minus_last4_min_row_seq").alias("PREV_CUSTOMER_ID_AVG_MONTHLY_FEE_MINUS_LAST4_MIN_ROW_SEQ_VAL"),
    )
    .unpivot(
        ids=["id"],
        values=["MONTHLY_FEE_PLUS_PREV_CUSTOMER_ID_AVG_MONTHLY_FEE_MINUS_LAST4_MIN_ROW_SEQ", "LAST4_MAX_ROW_SEQ_PERCENT_RANK_VAL", "PREV_CUSTOMER_ID_AVG_MONTHLY_FEE_MINUS_LAST4_MIN_ROW_SEQ_VAL"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
