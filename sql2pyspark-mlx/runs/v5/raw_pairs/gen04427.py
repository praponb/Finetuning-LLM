from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

lines = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

lines_carts = (
    lines
    .select(
        "customer_id",
        "product_id",
        "qty",
        "price",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

joined = (
    lines_carts
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select("e.customer_id", "e.qty", F.col("d.signup_date"))
)

with_windows = (
    joined
    .filter(F.col("customer_id").isNotNull())
    .select("customer_id", "qty", F.max("qty").over(w).alias("customer_id_max_qty"))
)

result = (
    with_windows
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("customer_id", "customer_id_max_qty", F.col("avg_account_balance"))
    .orderBy(F.col("avg_account_balance").desc())
)
