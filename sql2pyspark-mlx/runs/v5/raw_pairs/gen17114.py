from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-08-25").cast("date")) & (F.col("segment").eqNullSafe(F.lit("Consumer"))))
    .select("*", F.initcap(F.trim("name")).alias("name_title"))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") >= F.col("s.signup_date")), "left_semi")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"))
)

not_flagged_customers = (
    with_last
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.order_date") < "2025-11-01"), "left_semi")
)

not_flagged_customers_2 = (
    not_flagged_customers
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") < F.col("s.signup_date")), "left_semi")
)

result = (
    not_flagged_customers_2
    .groupBy("country")
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.corr("age", "credit_limit").alias("corr_age_credit_limit"),
    )
    .filter(F.col("corr_age_credit_limit") >= 0)
)
