from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("account_status").orderBy("rn", "id")
w4 = Window.partitionBy("city", "account_status").orderBy(F.col("rn_2").desc(), "id")
w5 = Window.partitionBy("account_account_type").orderBy("age", "id")
w6 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc())
w_grp = Window.orderBy(F.col("credit_limit").asc_nulls_last())
w8 = Window.partitionBy("city").orderBy("rn_2", "id")
w9 = Window.partitionBy("account_account_type").orderBy("rn_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2024-01-28").cast("date"))
    .withColumn("clean_email", F.trim("email"))
    .withColumn("clean_city", F.trim("city"))
    .withColumn("segment_filled", F.coalesce("segment", F.lit("NONE")))
)

latest = (
    cleaned
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

with_last = (
    latest
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.*",
        F.col("c.account_type").alias("account_account_type"),
        F.col("c.status").alias("account_status"),
    )
)

deduped = (
    with_last
    .withColumn("rn_2", F.row_number().over(w3))
    .filter(F.col("rn_2") == 1)
)

ranked = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_windows = (
    ranked
    .withColumn("age_bucket", F.ntile(3).over(w5))
    .withColumn("credit_limit_cume_dist", F.cume_dist().over(w6))
)

result = (
    with_windows
    .withColumn("credit_limit_rank", F.rank().over(w_grp))
    .withColumn("row_seq", F.row_number().over(w8))
    .withColumn("cumulative_age_bucket", F.sum("age_bucket").over(w9))
)
