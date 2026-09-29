from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

latest_match = (
    df_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.country",
        "s.segment",
        "s.signup_date",
        "s.is_active",
        "s.credit_limit",
        "s.age",
        F.col("c.amount").alias("order_amount"),
        F.col("c.discount"),
    )
)

result = (
    latest_match
    .rollup(
        F.date_trunc("MONTH", "signup_date").alias("activity_month"),
        F.when(F.col("discount") >= 0.33, "PREMIUM").otherwise("STANDARD").alias("discount_tier"),
    )
    .agg(
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
        F.countDistinct("country").alias("distinct_countries"),
        F.avg("age").alias("avg_age"),
    )
    .filter(F.col("jp_rows") < 1000)
    .orderBy(F.col("activity_month").desc_nulls_last())
)
