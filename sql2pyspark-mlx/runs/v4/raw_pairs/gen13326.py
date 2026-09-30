from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("status").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("store_id").orderBy("shipping_fee", "id")
w_hist = Window.partitionBy("customer_id").orderBy("amount", "id")
w4 = Window.orderBy("amount_bucket")
w5 = Window.partitionBy("channel", "store_id").orderBy("amount_bucket", "id")

valid = (
    df_orders
    .filter(F.col("channel").isNotNull() & F.col("customer_id").isNull())
    .select("*", F.trim("status").alias("clean_status"))
)

enriched = (
    valid
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.age"))
)

scored = (
    enriched
    .select(
        "*",
        F.sum("amount").over(w_grp).alias("cumulative_amount"),
        F.ntile(3).over(w_recent).alias("shipping_fee_bucket"),
        F.ntile(5).over(w_hist).alias("amount_bucket"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.cume_dist().over(w4).alias("amount_bucket_cume_dist"),
        F.ntile(10).over(w5).alias("amount_bucket_bucket"),
    )
)

result = (
    metrics
    .select(
        "id",
        "status",
        "amount",
        F.col("amount_bucket_cume_dist").alias("AMOUNT_BUCKET_CUME_DIST"),
    )
    .unpivot(
        ["id", "status"],
        ["amount", "AMOUNT_BUCKET_CUME_DIST"],
        "metric_name",
        "val",
    )
)
