from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("product_id").asc_nulls_last())
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)

flattened = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select("id", "customer_id", "created_at", "coupon_codes", F.col("elem.product_id"))
)

enriched = (
    flattened
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.credit_limit"))
)

metrics = (
    enriched
    .withColumn("product_id_cume_dist", F.cume_dist().over(w))
    .withColumn("customer_id_min_credit_limit", F.min("credit_limit").over(w2))
)

metrics_carts = (
    metrics
    .withColumn("product_id_cume_dist_count_2h", F.count("product_id_cume_dist").over(w3))
    .withColumn("customer_id_min_product_id_cume_dist", F.min("product_id_cume_dist").over(w2))
    .withColumn("customer_id_max_product_id_cume_dist", F.max("product_id_cume_dist").over(w2))
)

valid = (
    metrics_carts
    .filter(F.col("customer_id_max_product_id_cume_dist").between(0, 1))
    .withColumn("product_id_cume_dist_count_2h_adjusted", F.round((F.col("product_id_cume_dist_count_2h") * 100), 2))
)

scored = (
    valid
    .select(
        "*",
        F.max("customer_id_min_product_id_cume_dist").over(w4).alias("customer_id_min_product_id_cume_dist_max_1h"),
        F.max("product_id_cume_dist_count_2h").over(w2).alias("customer_id_max_product_id_cume_dist_count_2h"),
        F.max("customer_id_max_product_id_cume_dist").over(w2).alias("customer_id_max_customer_id_max_product_id_cume_dist"),
        F.sum("product_id_cume_dist_count_2h_adjusted").over(w2).alias("customer_id_sum_product_id_cume_dist_count_2h_adjusted"),
    )
)

joined = (
    scored
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.signup_date"))
)

grouped = (
    joined
    .groupBy("product_id", F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("cnt"),
        F.max("created_at").alias("last_created_at"),
        F.max("created_at").alias("last_seen"),
    )
)

result = (
    grouped
    .select("product_id", "hour_window", "cnt", "last_created_at")
    .orderBy("product_id", "last_created_at")
)
