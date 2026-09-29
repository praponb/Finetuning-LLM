from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("k").orderBy(F.col("credit_limit").asc_nulls_first())
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("discount").alias("sum_order_discount"))

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

enriched = (
    exploded
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.credit_limit"))
)

enriched_carts = (
    enriched
    .select(
        "id",
        "customer_id",
        "created_at",
        "v",
        F.dense_rank().over(w_part).alias("credit_limit_dense_rank"),
        F.count("credit_limit").over(w2).alias("customer_id_count_credit_limit"),
    )
)

with_windows = (
    enriched_carts
    .filter(F.col("customer_id_count_credit_limit") != 13)
    .withColumn("customer_id_min_customer_id_count_credit_limit", F.min("customer_id_count_credit_limit").over(w2))
    .withColumn("customer_id_max_customer_id_count_credit_limit", F.max("customer_id_count_credit_limit").over(w2))
    .withColumn("credit_limit_dense_rank_sum_2h", F.sum("credit_limit_dense_rank").over(w3))
    .withColumn("customer_id_count_credit_limit_share", F.round((F.col("customer_id_count_credit_limit") / F.sum("customer_id_count_credit_limit").over(w2)), 4))
)

enriched_carts_2 = (
    with_windows
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "e.customer_id",
        "e.created_at",
        "e.credit_limit_dense_rank",
        "e.customer_id_count_credit_limit",
        "e.customer_id_min_customer_id_count_credit_limit",
        "e.customer_id_max_customer_id_count_credit_limit",
        "e.credit_limit_dense_rank_sum_2h",
        F.col("b.signup_date"),
    )
)

result = (
    enriched_carts_2
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "credit_limit_dense_rank",
        "customer_id_count_credit_limit",
        "customer_id_min_customer_id_count_credit_limit",
        "customer_id_max_customer_id_count_credit_limit",
        "credit_limit_dense_rank_sum_2h",
        F.col("sum_order_discount"),
    )
    .orderBy("customer_id", "customer_id_min_customer_id_count_credit_limit")
)
