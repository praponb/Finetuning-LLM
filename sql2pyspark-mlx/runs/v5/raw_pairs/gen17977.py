from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("created_at", "id", "pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("price", "id", "pos")
w3 = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc(), "id", "pos")
w4 = Window.partitionBy("customer_id").orderBy("created_at", "id", "pos")
w5 = Window.partitionBy("customer_id").orderBy("created_at", "id", "pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "li_item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

joined = (
    exploded
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.created_at",
        "e.pos",
        "e.price",
        F.col("c.city"),
        F.col("c.name"),
    )
)

metrics = (
    joined
    .select(
        "*",
        F.sum("price").over(w).alias("running_price"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

cleaned = (
    metrics
    .filter(F.col("city").isNotNull() & (F.col("seq_no") == 7))
    .select(
        "*",
        (F.col("row_seq") + F.col("seq_no")).alias("row_seq_plus_seq_no"),
        F.round((F.col("row_seq") / F.col("price")), 2).alias("row_seq_per_price"),
    )
)

flagged = (
    cleaned
    .withColumn("new_group_flag", F.when(F.lag("created_at").over(w4).isNull() | (F.col("created_at") > F.lag("created_at").over(w4) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w5))

base = (
    islands
    .filter(F.col("seq_no").between(5, 7))
    .select(
        "*",
        F.when(F.col("row_seq_plus_seq_no") >= 85, "High").when(F.col("row_seq_plus_seq_no") >= 34, "Medium").otherwise("Low").alias("row_seq_plus_seq_no_tier"),
    )
)

result = (
    base
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .cube(F.col("d.country").alias("country"), F.col("a.city").alias("cart_city"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("a.row_seq_per_price")).alias("avg_row_seq_per_price"),
        F.grouping_id().alias("gid"),
    )
)
