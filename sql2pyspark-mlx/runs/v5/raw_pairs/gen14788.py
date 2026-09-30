from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code")
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("city")
w_grp = Window.partitionBy("city").orderBy(F.col("code_avg_credit_limit").desc_nulls_last())
w5 = Window.partitionBy("country")

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

enriched = (
    exploded
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.country") == "DE")
    .select("s.*", F.col("b.age"), F.col("b.country"), F.col("b.credit_limit"))
)

enriched_carts = (
    enriched
    .select(
        "id",
        "customer_id",
        F.avg("credit_limit").over(w).alias("code_avg_credit_limit"),
        F.max("age").over(w2).alias("age_max_30m"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4).alias("credit_limit_share"),
    )
)

joined = (
    enriched_carts
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.code_avg_credit_limit",
        "s.credit_limit_share",
        F.col("b.name"),
        F.col("b.city"),
        F.col("b.country"),
    )
)

matched_keys = (
    joined
    .select("customer_id")
    .exceptAll(
        df_carts
        .filter(F.size("coupon_codes") > 2)
        .select("customer_id")
    )
)

verified = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "code_avg_credit_limit",
        "credit_limit_share",
        F.round((F.col("code_avg_credit_limit") / F.sum("code_avg_credit_limit").over(w3)), 4).alias("code_avg_credit_limit_share"),
        F.percent_rank().over(w_grp).alias("code_avg_credit_limit_percent_rank"),
        F.min("credit_limit_share").over(w5).alias("country_min_credit_limit_share"),
    )
)
