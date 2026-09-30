from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w2 = Window.partitionBy("city", "country").orderBy(F.col("credit_limit").desc_nulls_last())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("city").orderBy(F.col("balance").asc_nulls_last(), "id")

scored = (
    df_customers
    .withColumn("segment_count_age", F.count("age").over(w))
    .withColumn("credit_limit_percent_rank", F.percent_rank().over(w2))
)

latest_match = (
    scored
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("account_status"), F.col("c.balance"))
)

with_last = (
    latest_match
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("order_amount"), F.col("c.channel"))
)

latest = (
    with_last
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        "segment_count_age",
        F.when(F.col("segment").rlike("^(Consumer|SMB)$"), "EXTENDED").when(F.col("segment").rlike("^(Corporate)$"), "PRIORITY").otherwise("GROUP_B").alias("segment_zone"),
    )
    .orderBy(F.col("segment_zone").desc_nulls_first())
)
