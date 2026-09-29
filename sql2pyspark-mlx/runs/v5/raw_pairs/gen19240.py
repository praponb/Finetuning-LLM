from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("cost", "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("order_id", "status")
w4 = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id", "carrier").orderBy(F.col("cost_share").asc_nulls_last())

ranked = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

ranked_shipments = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

metrics = (
    ranked_shipments
    .filter(F.col("rn") > 1)
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        F.round((F.col("cost") / F.sum("cost").over(w3)), 4).alias("cost_share"),
        F.first("carrier", ignorenulls=True).over(w4).alias("first_known_carrier"),
    )
)

deduped = (
    metrics
    .withColumn("rn", F.rank().over(w5))
    .filter(F.col("rn") == 1)
    .select("id", "carrier", "cost", "cost_share", "first_known_carrier")
)

result = (
    deduped
    .select("id", "carrier", "cost", "cost_share", "first_known_carrier")
    .orderBy(F.col("id").desc(), "carrier")
)
