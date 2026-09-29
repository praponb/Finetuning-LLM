from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_type", "status").orderBy(F.col("balance").desc(), "id")
w3 = Window.orderBy("event_seq", "id")
w_grp = Window.partitionBy("account_type").orderBy("opened_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id")
w6 = Window.partitionBy("customer_id").orderBy(F.col("opened_date_month_end").desc_nulls_first(), "id")

filtered = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "status", "account_type").alias("status_account_type_key"),
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.first("status", ignorenulls=True).over(w).alias("first_known_status"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

enriched = (
    metrics
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.name"))
)

valid = (
    enriched
    .filter(F.col("customer_id").isNotNull())
    .withColumn("account_type_title", F.initcap(F.trim("account_type")))
    .withColumn("account_type_upper", F.upper("account_type"))
)

enriched_accounts = (
    valid
    .select(
        "*",
        F.lag("balance", 2, 0).over(w3).alias("prev_balance"),
        F.sum("balance").over(w_grp).alias("running_balance"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w5)), 4).alias("event_seq_share"),
    )
)

latest = (
    enriched_accounts
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy("customer_id")
    .pivot("account_type", ["LOAN", "SAVINGS"])
    .agg(F.sum("prev_balance").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
