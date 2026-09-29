from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id")
w_part_2 = Window.partitionBy("value", "customer_id")
w3 = Window.partitionBy("value")
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("amount").alias("min_order_amount"))

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

lines_carts = (
    lines
    .select(
        "id",
        "customer_id",
        "attr_key",
        "v",
        F.explode("coupon_codes").alias("value"),
    )
)

valid = (
    lines_carts
    .filter(F.col("value").isNotNull())
    .withColumn("attr_key_upper", F.upper("attr_key"))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("NONE")))
)

with_dims = (
    valid
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.credit_limit"), F.col("d.signup_date"))
)

enriched = (
    with_dims
    .select(
        "customer_id",
        "attr_key_upper",
        "credit_limit",
        F.avg("credit_limit").over(w_part).alias("customer_id_avg_credit_limit"),
        F.max("credit_limit").over(w_part_2).alias("value_max_credit_limit"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w_part)), 4).alias("credit_limit_share"),
        F.avg("credit_limit").over(w3).alias("value_avg_credit_limit"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w3)), 4).alias("credit_limit_share_2"),
    )
)

result = (
    enriched
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "attr_key_upper",
        "credit_limit",
        "customer_id_avg_credit_limit",
        "credit_limit_share",
        "value_avg_credit_limit",
        "credit_limit_share_2",
        F.col("min_order_amount"),
    )
    .orderBy("credit_limit_share")
)
