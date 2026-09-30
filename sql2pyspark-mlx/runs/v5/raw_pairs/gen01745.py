from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("credit_limit", "id")
w2 = Window.partitionBy("country")
w3 = Window.orderBy(F.col("prev_age").desc_nulls_first())
w4 = Window.partitionBy("city", "country")
w5 = Window.partitionBy("city")
w6 = Window.partitionBy("country").orderBy("credit_limit_share", "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

cleaned = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select("*", F.upper("city").alias("city_upper"))
)

kept = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") < F.col("s.signup_date")), "left_semi")
)

enriched = (
    kept
    .select(
        "*",
        F.lag("age", 2).over(w).alias("prev_age"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4).alias("credit_limit_share"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.plan") == "premium"), "left_anti")
)

metrics = (
    not_flagged
    .filter(F.col("credit_limit").between(9100, 13500))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.percent_rank().over(w3).alias("prev_age_percent_rank"),
        F.corr("credit_limit", "age").over(w4).alias("corr_credit_limit_age"),
        F.corr("age", "credit_limit").over(w5).alias("corr_age_credit_limit"),
        F.ntile(10).over(w6).alias("credit_limit_share_bucket"),
    )
)

latest_match = (
    metrics
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount"))
)

result = (
    latest_match
    .select("id", "city", "corr_age_credit_limit", "prev_age_percent_rank")
    .unpivot(
        ids=["id", "city"],
        values=["corr_age_credit_limit", "prev_age_percent_rank"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
