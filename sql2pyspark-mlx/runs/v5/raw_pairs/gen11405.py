from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("country").orderBy("age", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("signup_date", "id")
w4 = Window.partitionBy("country").orderBy("account_opened_date", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("country").orderBy("account_opened_date", "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

with_last = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.country",
        "s.signup_date",
        "s.is_active",
        "s.credit_limit",
        "s.age",
        F.col("c.opened_date").alias("account_opened_date"),
    )
)

enriched = (
    with_last
    .select(
        "*",
        F.avg("age").over(w_ordered).alias("last3_avg_age"),
        F.count("age").over(w_ordered).alias("last3_count_age"),
        F.sum("age").over(w_ordered).alias("last3_sum_age"),
        F.lead("age", 1).over(w3).alias("next_age"),
    )
)

enriched_customers = (
    enriched
    .select(
        "*",
        F.avg("last3_sum_age").over(w4).alias("last3_sum_age_avg_3_prior"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

with_latest = (
    enriched_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount").alias("order_discount"), F.col("c.status"))
)

with_latest_customers = (
    with_latest
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.country",
        "s.is_active",
        "s.credit_limit",
        "s.age",
        "s.last3_count_age",
        "s.row_seq",
        "s.order_discount",
        "s.status",
        F.col("c.channel"),
        F.col("c.discount"),
    )
)

result = (
    with_latest_customers
    .select(
        "id",
        "country",
        F.col("credit_limit").alias("CREDIT_LIMIT"),
        F.col("order_discount").alias("ORDER_DISCOUNT_VAL"),
        "discount",
    )
    .unpivot(
        ids=["id", "country"],
        values=["CREDIT_LIMIT", "ORDER_DISCOUNT_VAL", "discount"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
