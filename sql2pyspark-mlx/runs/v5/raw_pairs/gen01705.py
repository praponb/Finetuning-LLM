from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id", "attr_key")
w3 = Window.orderBy(F.col("credit_limit").asc_nulls_first())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

flattened = (
    exploded
    .select(
        "id",
        "customer_id",
        "attr_key",
        "v",
        F.explode("coupon_codes").alias("element"),
    )
)

enriched = (
    flattened
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.attr_key",
        "e.v",
        "e.element",
        F.col("d.credit_limit"),
        F.col("d.email"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "customer_id",
        "attr_key",
        "v",
        "element",
        "email",
        F.avg("credit_limit").over(w).alias("customer_id_avg_credit_limit"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4).alias("credit_limit_share"),
        F.percent_rank().over(w3).alias("credit_limit_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "customer_id",
        F.col("credit_limit_share").alias("CREDIT_LIMIT_SHARE"),
        F.col("credit_limit_percent_rank").alias("CREDIT_LIMIT_PERCENT_RANK"),
    )
    .unpivot(
        ids=["customer_id"],
        values=["CREDIT_LIMIT_SHARE", "CREDIT_LIMIT_PERCENT_RANK"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
