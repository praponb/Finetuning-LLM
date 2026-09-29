from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w2 = Window.orderBy(F.col("age").asc_nulls_last())
w3 = Window.orderBy("account_balance")
w4 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w6 = Window.orderBy("credit_limit", "id")
w7 = Window.partitionBy("country").orderBy("age_dense_rank", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.partitionBy("order_status", "country").orderBy("credit_limit", "id").rowsBetween(-6, -1)
w9 = Window.partitionBy("city").orderBy("age_dense_rank")

latest_match = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.balance").alias("account_balance"))
)

scored = (
    latest_match
    .select(
        "id",
        "email",
        "country",
        "city",
        "is_active",
        "credit_limit",
        "age",
        F.dense_rank().over(w2).alias("age_dense_rank"),
        F.dense_rank().over(w3).alias("account_balance_dense_rank"),
        F.avg("account_balance").over(w4).alias("account_balance_avg_5"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.status").alias("order_status"),
        F.col("c.channel").alias("order_channel"),
    )
)

with_windows = (
    with_last
    .select(
        "*",
        F.lag("age_dense_rank", 2, 0).over(w6).alias("prev_age_dense_rank"),
        F.avg("age_dense_rank").over(w7).alias("age_dense_rank_avg_5"),
        F.avg("age").over(w8).alias("age_avg_6_prior"),
        F.percent_rank().over(w9).alias("age_dense_rank_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "email",
        "city",
        "is_active",
        "credit_limit",
        "age",
        "age_dense_rank",
        "account_balance_dense_rank",
        "order_status",
        F.when(F.col("city").rlike("^(London|Singapore)$"), "CORE").otherwise("PRIORITY").alias("city_zone"),
    )
    .orderBy(F.col("order_status").desc_nulls_last(), "id")
)
