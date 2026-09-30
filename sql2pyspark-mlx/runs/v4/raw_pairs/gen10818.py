from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("activity_month").orderBy(F.col("age_bit_and").desc())

with_last = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.opened_date").alias("account_opened_date"),
        F.col("c.status"),
    )
)

result = (
    with_last
    .groupBy(
        F.date_trunc("month", "account_opened_date").alias("activity_month"),
        "country",
        "city",
    )
    .agg(
        F.percentile_approx("age", 0.5).alias("age_p50"),
        F.min("credit_limit").alias("min_credit_limit"),
        F.bit_and("age").alias("age_bit_and"),
    )
    .withColumn("age_bit_and_rank", F.rank().over(w2))
    .orderBy(F.col("city").desc_nulls_last())
)
