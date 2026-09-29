from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("city").orderBy("start_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("first_known_credit_limit", "id")
w4 = Window.partitionBy("customer_id").orderBy("seq_no", "id")
w5 = Window.partitionBy("customer_id")
w6 = Window.partitionBy("customer_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.start_date",
        F.col("c.email"),
        F.col("c.credit_limit"),
        F.col("c.city"),
    )
)

metrics = (
    with_dims
    .withColumn("last4_min_credit_limit", F.min("credit_limit").over(w_hist))
    .withColumn("last4_avg_credit_limit", F.avg("credit_limit").over(w_hist))
    .withColumn("last4_count_credit_limit", F.count("credit_limit").over(w_hist))
    .withColumn("first_known_credit_limit", F.first("credit_limit", ignorenulls=True).over(w2))
)

scored = (
    metrics
    .select(
        "id",
        "customer_id",
        "start_date",
        "email",
        "credit_limit",
        "last4_avg_credit_limit",
        F.lead("first_known_credit_limit").over(w3).alias("next_first_known_credit_limit"),
        F.lead("last4_min_credit_limit").over(w3).alias("next_last4_min_credit_limit"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

enriched = (
    scored
    .withColumn("seq_no_bucket", F.ntile(5).over(w4))
    .withColumn("last4_avg_credit_limit_share", F.round((F.col("last4_avg_credit_limit") / F.sum("last4_avg_credit_limit").over(w5)), 4))
    .withColumn("first_known_next_last4_min_credit_limit", F.first("next_last4_min_credit_limit", ignorenulls=True).over(w6))
)

joined = (
    enriched
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.signup_date"), F.col("p.country"), F.col("p.age"))
)

result = (
    joined
    .select(
        "id",
        "customer_id",
        "email",
        "seq_no",
        "seq_no_bucket",
        "first_known_next_last4_min_credit_limit",
        "signup_date",
    )
    .orderBy(F.col("id").asc_nulls_last())
)
