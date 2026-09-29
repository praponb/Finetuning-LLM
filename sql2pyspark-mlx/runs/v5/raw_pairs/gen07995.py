from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.sum("revenue").alias("sum_store_sale_revenue"))

with_latest = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.order_date",
        "s.status",
        "s.amount",
        "s.discount",
        F.col("c.paid_at").alias("payment_paid_at"),
    )
)

result = (
    with_latest
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        F.col("sum_store_sale_revenue"),
    )
    .orderBy(F.col("store_id").desc_nulls_first(), "sum_store_sale_revenue")
)
