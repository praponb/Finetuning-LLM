from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("credit_limit").desc_nulls_last())
w2 = Window.partitionBy("city").orderBy(F.col("is_consumer").asc_nulls_first())
w3 = Window.partitionBy("segment", "city").orderBy(F.col("least_credit_limit_age").desc(), "id")
w4 = Window.partitionBy("country", "city")
w5 = Window.partitionBy("segment", "country").orderBy("credit_limit_cume_dist", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "credit_limit",
        F.when(F.col("segment") == "Consumer", 1).otherwise(0).alias("is_consumer"),
        F.least("credit_limit", "age").alias("least_credit_limit_age"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

enriched = (
    base
    .filter(F.col("credit_limit").between(6900, 9100))
    .select(
        "id",
        "email",
        "country",
        "segment",
        "least_credit_limit_age",
        "city_filled",
        F.cume_dist().over(w).alias("credit_limit_cume_dist"),
        F.cume_dist().over(w2).alias("is_consumer_cume_dist"),
        F.row_number().over(w3).alias("event_seq"),
        F.max("is_consumer").over(w4).alias("country_max_is_consumer"),
    )
)

scored = (
    enriched
    .withColumn("last7_avg_is_consumer_cume_dist", F.avg("is_consumer_cume_dist").over(w5))
    .withColumn("last7_sum_is_consumer_cume_dist", F.sum("is_consumer_cume_dist").over(w5))
    .withColumn("last7_max_is_consumer_cume_dist", F.max("is_consumer_cume_dist").over(w5))
)

kept = (
    scored
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.shipping_fee") >= 39), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.customer_id") == 40), "left_anti")
)

result = (
    not_flagged
    .groupBy("segment")
    .pivot("country", ["JP", "SG", "UK"])
    .agg(
        F.sum("least_credit_limit_age").alias("value_sum"),
        F.count(F.lit(1)).alias("n"),
    )
)
