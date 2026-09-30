from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "code")
w_grp = Window.partitionBy("customer_id").orderBy(F.col("price").desc())
w_grp_2 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("code")
w5 = Window.orderBy(F.col("product_id").desc())
w6 = Window.orderBy(F.col("price_cume_dist").asc_nulls_last())

flattened = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "elem"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "pos",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
    )
)

flattened_carts = (
    flattened
    .select(
        "id",
        "customer_id",
        "pos",
        "product_id",
        "qty",
        "price",
        F.explode("coupon_codes").alias("code"),
    )
)

joined = (
    flattened_carts
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.pos",
        "a.product_id",
        "a.price",
        "a.code",
        F.col("b.signup_date"),
    )
)

scored = (
    joined
    .filter(F.col("code") == "WELCOME5")
    .select(
        "customer_id",
        "pos",
        "product_id",
        "price",
        "code",
        F.avg("price").over(w).alias("customer_id_avg_price"),
        F.cume_dist().over(w_grp).alias("price_cume_dist"),
        F.sum("product_id").over(w_grp_2).alias("customer_id_sum_product_id"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.count("pos").over(w4).alias("code_count_pos"),
        F.rank().over(w5).alias("product_id_rank"),
        F.dense_rank().over(w6).alias("price_cume_dist_dense_rank"),
    )
)

totals = (
    metrics
    .groupBy(
        "product_id",
        "code",
        F.when(F.col("price") >= 195, "LARGE").otherwise("SMALL").alias("price_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("cnt"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("code") == "SPRING10", 1)).alias("spring10_rows"),
        F.round(F.sum("price") / F.nullif(F.count(F.when(F.col("code") == "SPRING10", 1)), F.lit(0)), 2).alias("price_per_spring10"),
    )
)

result = (
    totals
    .select(
        "product_id",
        "code",
        "price_tier",
        "cnt",
        "row_count",
        "spring10_rows",
        "price_per_spring10",
    )
    .orderBy(F.col("cnt").desc_nulls_last())
)
