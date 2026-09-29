from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("signup_date").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("city", "segment").orderBy(F.col("amount").desc())
w4 = Window.orderBy(F.col("amount").asc_nulls_last())
w_ordered = Window.orderBy("amount", "id")

filtered = (
    df_customers
    .filter(~F.col("is_active"))
    .select(
        "id",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
        F.coalesce("segment", F.lit("OTHER")).alias("segment_filled"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") < F.col("s.signup_date")), "left_anti")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.segment",
        "s.email_user",
        "s.segment_filled",
        "s.row_num",
        F.col("c.amount"),
    )
)

scored = (
    with_last
    .select(
        "id",
        "city",
        "email_user",
        "segment_filled",
        F.percent_rank().over(w_hist).alias("amount_percent_rank"),
        F.dense_rank().over(w4).alias("amount_dense_rank"),
        F.lead("amount", 1).over(w_ordered).alias("next_amount"),
    )
)

result = (
    scored
    .select("id", "email_user", "segment_filled", "amount_dense_rank", "next_amount")
)
