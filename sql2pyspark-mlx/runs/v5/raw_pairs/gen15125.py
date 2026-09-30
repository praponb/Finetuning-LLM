from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "channel").orderBy("order_date", "id").rowsBetween(-3, -1)
w2 = Window.orderBy(F.col("order_date").desc(), "id")
w_grp = Window.partitionBy("channel").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.min("revenue").alias("min_store_sale_revenue"))

scored = (
    df_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "channel",
        "amount",
        F.avg("discount").over(w).alias("discount_avg_3_prior"),
        F.row_number().over(w2).alias("event_seq"),
        F.first("channel", ignorenulls=True).over(w_grp).alias("first_known_channel"),
    )
)

result = (
    scored
    .join(revenue_by_store_id, on="store_id", how="left")
    .select("id", "store_id", "event_seq", F.col("min_store_sale_revenue"))
    .orderBy(F.col("store_id").desc(), "event_seq")
)
