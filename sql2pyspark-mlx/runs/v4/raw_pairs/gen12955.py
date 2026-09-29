from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("cost").desc(), "id")
w_grp = Window.partitionBy("status").orderBy("cost", "id")
w3 = Window.partitionBy("status").orderBy("status_length", "id")
w4 = Window.partitionBy("carrier").orderBy(F.col("rank_in_group").desc(), "id")
w5 = Window.partitionBy("status", "carrier").orderBy(F.col("status_length").desc_nulls_first(), "id")
w6 = Window.partitionBy("status", "order_id").orderBy("prev_status_length", "id")
w_part = Window.partitionBy("status", "carrier").orderBy("cost", "id")
w8 = Window.partitionBy("status", "carrier").orderBy(F.col("status_length_bucket").asc_nulls_last())
w9 = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc())

filtered = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & F.col("carrier").isin(["DHL", "UPS"]))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.length("status").alias("status_length"),
        F.upper("status").alias("status_upper"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    ranked
    .withColumn("prev_status_length", F.lag("status_length", 1).over(w_grp))
    .withColumn("status_length_bucket", F.ntile(5).over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
)

with_dims = (
    scored
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.discount"))
)

deduped = (
    with_dims
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

scored_shipments = (
    deduped
    .select(
        "*",
        F.lag("cost", 1).over(w6).alias("prev_cost"),
        F.lag("row_seq", 2).over(w_part).alias("prev_row_seq"),
        F.percent_rank().over(w8).alias("status_length_bucket_percent_rank"),
    )
)

latest = (
    scored_shipments
    .withColumn("dedup_rank", F.rank().over(w9))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .select("id", "order_id", "cost", "prev_cost", "discount")
    .unpivot(["id", "order_id"], ["cost", "prev_cost", "discount"], "metric", "val")
    .filter(F.col("val").isNotNull())
)
