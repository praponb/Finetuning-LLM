from pyspark.sql import functions as F

kept_keys = (
    df_orders
    .select("status", "id")
    .subtract(
        df_orders
        .filter(F.col("discount") < 0.38)
        .select("status", "id")
    )
)

verified = (
    df_orders
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "customer_id", "status", "shipping_fee")
    .orderBy("customer_id", F.col("status").desc())
)
