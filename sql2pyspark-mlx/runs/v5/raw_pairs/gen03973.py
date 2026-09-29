from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount")
w2 = Window.partitionBy("order_id", "status").orderBy(F.col("order_amount").desc(), "id")

deduped = (
    df_payments
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "order_id", "method", "amount", "paid_at")
)

with_dims = (
    deduped
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.order_date") > "2023-04-06")
    .select(
        "a.id",
        "a.order_id",
        F.col("p.amount").alias("order_amount"),
        F.col("p.order_date"),
        F.col("p.status"),
    )
)

joined = (
    with_dims
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id").eqNullSafe(F.col("o.id")), "inner")
    .select("src.*", F.col("o.discount"))
)

ranked = (
    joined
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "order_amount",
        "order_date",
        "status",
        "discount",
        F.when(F.col("status").rlike("^(SHIPPED|COMPLETED)$"), "PRIORITY").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy(F.col("order_date").desc(), F.col("status").desc())
)
