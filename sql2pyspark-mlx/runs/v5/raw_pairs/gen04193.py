from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc(), "id", "key")
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("shipping_fee").alias("sum_order_shipping_fee"))

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "attr_value"),
    )
)

latest = (
    flattened
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

kept_keys = (
    latest
    .select("id", "customer_id")
    .intersectAll(
        df_carts
        .filter(F.size("coupon_codes") > 1)
        .select("id", "customer_id")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .select(
        "src.customer_id",
        "src.created_at",
        "src.key",
        F.col("c.city"),
        F.col("c.age"),
        F.col("c.name"),
    )
)

wide = (
    joined
    .groupBy("customer_id")
    .pivot("city", ["London", "Berlin", "New York"])
    .agg(F.avg("age").alias("value_sum"), F.count(F.lit(1)).alias("n"))
    .select(
        "customer_id",
        F.col("London_value_sum").alias("london_value_sum"),
        F.col("London_n").alias("london_n"),
        F.col("Berlin_value_sum").alias("berlin_value_sum"),
        F.col("Berlin_n").alias("berlin_n"),
        F.col("New York_value_sum").alias("new_york_value_sum"),
        F.col("New York_n").alias("new_york_n"),
    )
)

result = (
    wide
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "london_value_sum",
        "london_n",
        "berlin_n",
        "new_york_value_sum",
        F.col("sum_order_shipping_fee"),
    )
    .orderBy(F.col("customer_id").desc())
)
