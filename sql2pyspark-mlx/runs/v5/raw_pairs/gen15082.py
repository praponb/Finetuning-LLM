from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")
w2 = Window.partitionBy("city")
w3 = Window.orderBy(F.col("credit_limit").desc_nulls_first())
w4 = Window.orderBy("credit_limit_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("credit_limit_dense_rank").desc(), "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w8 = Window.partitionBy("status").orderBy(F.col("credit_limit_share").desc(), "id")

cleaned = (
    df_customers
    .filter(F.col("credit_limit") <= 12300)
    .select(
        "*",
        F.when(F.col("city").isin(["Tokyo", "Chiang Mai"]), "Core").otherwise("Other").alias("city_group"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

metrics = (
    cleaned
    .filter(F.col("signup_date") >= "2024-10-01")
    .select(
        "id",
        "name",
        "email",
        "credit_limit",
        "age",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4).alias("credit_limit_share"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
        F.dense_rank().over(w3).alias("credit_limit_dense_rank"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "name",
        "credit_limit_share",
        F.last("credit_limit_dense_rank", ignorenulls=True).over(w4).alias("last_known_credit_limit_dense_rank"),
        F.last("credit_limit_share", ignorenulls=True).over(w4).alias("last_known_credit_limit_share"),
        F.sum("credit_limit").over(w5).alias("running_credit_limit"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"), F.col("c.amount").alias("order_amount"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.customer_id").isin([6, 13, 32, 34])), "left_anti")
)

ranked = (
    not_flagged
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    ranked
    .withColumn("last_known_credit_limit_share_tier", F.when(F.col("last_known_credit_limit_share") >= 0.77, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["last_known_credit_limit_share_tier", "status"], ["last_known_credit_limit_share_tier"], []],
        "last_known_credit_limit_share_tier",
        "status",
    )
    .agg(
        F.avg("last_known_credit_limit_dense_rank").alias("avg_last_known_credit_limit_dense_rank"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("last_known_credit_limit_dense_rank")).otherwise(0)).alias("pending_last_known_credit_limit_dense_rank"),
        F.avg("last_known_credit_limit_dense_rank").alias("avg_last_known_credit_limit_dense_rank_2"),
        F.max(F.when(F.col("status") == "COMPLETED", F.col("credit_limit_share"))).alias("completed_max_credit_limit_share"),
        F.countDistinct("status").alias("distinct_status"),
    )
)
