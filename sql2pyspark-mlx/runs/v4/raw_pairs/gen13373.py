from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").asc_nulls_first())
w2 = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("status").orderBy("shipped_date", "id")
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-03-21").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.dayofmonth("delivered_date").alias("delivered_date_day"),
        F.coalesce("carrier", F.lit("NONE")).alias("resolved_carrier"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    ranked
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .withColumn("prev_cost", F.lag("cost", 1).over(w3))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w4)), 4))
    .withColumn("cumulative_delivered_date_day", F.sum("delivered_date_day").over(w5))
)

result = (
    metrics
    .select(
        "id",
        "delivered_date",
        "cost",
        "status",
        "delivered_date_day",
        "row_num",
        "prev_cost",
        "cost_share",
        "cumulative_delivered_date_day",
        F.when(F.col("status").rlike("^(LOST|IN_TRANSIT)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy(F.col("status").desc_nulls_last())
)
