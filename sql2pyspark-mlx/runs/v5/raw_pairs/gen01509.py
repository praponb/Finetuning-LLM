from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("cost").asc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("delivered_date").desc())
w_ordered = Window.partitionBy("status").orderBy("shipped_date", "id")
w_ordered_2 = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")
w5 = Window.partitionBy("order_status", "order_id").orderBy("amount")
w6 = Window.orderBy(F.col("amount").desc(), "id")
w7 = Window.partitionBy("order_status")

joined = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.status") == "CANCELLED")
    .select(
        "a.*",
        F.col("b.order_date"),
        F.col("b.amount"),
        F.col("b.status").alias("order_status"),
    )
)

latest = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    latest
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows = (
    deduped
    .select(
        "id",
        "carrier",
        "status",
        "order_date",
        "amount",
        "order_status",
        F.lead("cost", 1).over(w_ordered).alias("next_cost"),
        F.ntile(5).over(w_ordered_2).alias("cost_bucket"),
        F.rank().over(w5).alias("amount_rank"),
        F.ntile(5).over(w6).alias("amount_bucket"),
        F.round((F.col("amount") / F.sum("amount").over(w7)), 4).alias("amount_share"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "order_status",
        F.col("amount").alias("AMOUNT"),
        "next_cost",
        F.col("amount_share").alias("AMOUNT_SHARE"),
    )
    .unpivot(
        ids=["id", "order_status"],
        values=["AMOUNT", "next_cost", "AMOUNT_SHARE"],
        variableColumnName="measure",
        valueColumnName="val",
    )
)
