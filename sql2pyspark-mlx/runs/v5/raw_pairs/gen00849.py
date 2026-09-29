from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy("age", "id")
w2 = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc(), "id")
w3 = Window.orderBy("next_credit_limit_filled", "id")
w4 = Window.orderBy("next_credit_limit_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("next_credit_limit_filled").desc(), "id")
w6 = Window.orderBy("row_seq", "id")
w7 = Window.orderBy("prev_credit_limit_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("prev_credit_limit_bucket", "id")
w9 = Window.orderBy("next_credit_limit_filled_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy(F.col("last_known_credit_limit_filled").desc(), "id")
w11 = Window.orderBy(F.col("last_known_prev_credit_limit").desc_nulls_first())
w12 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w13 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

base = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
)

filtered = (
    base
    .filter(~F.col("segment").eqNullSafe(F.lit("Corporate")))
    .withColumn("credit_limit_filled", F.coalesce("credit_limit", F.lit(0)))
    .withColumn("segment_clean", F.coalesce("segment", F.lit("DIRECT")))
)

with_windows = (
    filtered
    .select(
        "id",
        "is_active",
        "email_user",
        "credit_limit_filled",
        F.lag("credit_limit_filled").over(w).alias("prev_credit_limit_filled"),
        F.lead("credit_limit_filled").over(w).alias("next_credit_limit_filled"),
        F.lag("credit_limit").over(w).alias("prev_credit_limit"),
        F.row_number().over(w).alias("row_seq"),
        F.ntile(3).over(w2).alias("credit_limit_bucket"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "email_user",
        "credit_limit_filled",
        "prev_credit_limit_filled",
        "next_credit_limit_filled",
        F.lag("credit_limit_bucket", 1).over(w3).alias("prev_credit_limit_bucket"),
        F.last("prev_credit_limit", ignorenulls=True).over(w4).alias("last_known_prev_credit_limit"),
        F.ntile(3).over(w5).alias("next_credit_limit_filled_bucket"),
        F.ntile(10).over(w6).alias("row_seq_bucket"),
        F.last("credit_limit_filled", ignorenulls=True).over(w7).alias("last_known_credit_limit_filled"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.ntile(4).over(w8).alias("prev_credit_limit_bucket_bucket"),
        F.sum("next_credit_limit_filled_bucket").over(w9).alias("cumulative_next_credit_limit_filled_bucket"),
        F.row_number().over(w10).alias("event_seq"),
        F.percent_rank().over(w11).alias("last_known_prev_credit_limit_percent_rank"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w12))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.credit_limit_filled",
        "s.next_credit_limit_filled",
        "s.last_known_prev_credit_limit",
        "s.row_seq_bucket",
        "s.last_known_credit_limit_filled",
        "s.prev_credit_limit_bucket_bucket",
        "s.event_seq",
        "s.last_known_prev_credit_limit_percent_rank",
        F.col("c.opened_date").alias("account_opened_date"),
    )
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w13))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.credit_limit_filled",
        "s.next_credit_limit_filled",
        "s.row_seq_bucket",
        "s.last_known_credit_limit_filled",
        F.col("c.order_date"),
        F.col("c.shipping_fee").alias("order_shipping_fee"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "credit_limit_filled",
        "next_credit_limit_filled",
        "last_known_credit_limit_filled",
        "order_date",
        "order_shipping_fee",
    )
    .orderBy(F.col("last_known_credit_limit_filled").desc_nulls_first())
)
