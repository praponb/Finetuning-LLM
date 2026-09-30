from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country", "city")
w5 = Window.partitionBy("segment", "country").orderBy("credit_limit", "id")
w6 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")

enriched = (
    df_customers
    .withColumn("running_age", F.sum("age").over(w_part))
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w2)), 4))
    .withColumn("cumulative_credit_limit", F.sum("credit_limit").over(w3))
)

metrics = (
    enriched
    .select(
        "*",
        F.avg("running_age").over(w2).alias("city_avg_running_age"),
        F.min("credit_limit").over(w4).alias("country_min_credit_limit"),
        F.ntile(3).over(w5).alias("credit_limit_bucket"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.signup_date")), "left_semi")
)

eligible_customers = (
    eligible
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.shipping_fee") > 40), "left_anti")
)

result = (
    eligible_customers
    .select(
        "id",
        "city",
        F.col("age_share").alias("AGE_SHARE"),
        F.col("credit_limit").alias("CREDIT_LIMIT"),
    )
    .unpivot(
        ids=["id", "city"],
        values=["AGE_SHARE", "CREDIT_LIMIT"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
