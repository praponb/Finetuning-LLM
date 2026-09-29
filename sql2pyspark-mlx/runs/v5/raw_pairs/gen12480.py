from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "promo")

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "elem"))
    .select(
        "id",
        "customer_id",
        "created_at",
        (F.col("pos") + 1).alias("line_no"),
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

scored = flattened.select("*", F.max("price").over(w).alias("customer_id_max_price"))

with_dims = (
    scored
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .filter(F.col("p.signup_date") < "2023-06-01")
    .select("s.*", F.col("p.age"), F.col("p.city"))
)

valid = (
    with_dims
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-02"))) & (F.col("city").eqNullSafe(F.lit("Bangkok"))))
    .select(
        "id",
        "customer_id",
        "created_at",
        "line_no",
        "product_id",
        "qty",
        "promo",
        "customer_id_max_price",
        "age",
        "city",
        F.when(F.col("line_no") >= 4, "High").when(F.col("line_no") >= 2, "Medium").otherwise("Low").alias("line_no_level"),
        (F.col("line_no") - F.col("customer_id_max_price")).alias("line_no_minus_customer_id_max_price"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

base = (
    valid
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-02 03:00:00"))) & F.col("customer_id").isNotNull())
    .withColumn("line_no_minus_customer_id_max_price_minus_line_no", (F.col("line_no_minus_customer_id_max_price") - F.col("line_no")))
)

result = (
    base
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.city").alias("customer_city"), F.col("s.city").alias("cart_city"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("s.line_no_minus_customer_id_max_price")).alias("min_line_no_minus_customer_id_max_price"),
        F.round(F.sum(F.col("s.qty")), 2).alias("total_qty"),
    )
)
