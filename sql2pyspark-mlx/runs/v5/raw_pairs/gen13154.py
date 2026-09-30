from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("region").orderBy("opened_date", "id")
w3 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("prev_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("sqft", "id")
w6 = Window.partitionBy("region").orderBy(F.col("running_sqft").desc(), "id")
w7 = Window.partitionBy("city").orderBy("sqft", "id")
w8 = Window.orderBy("row_seq_2", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.partitionBy("city").orderBy("running_sqft", "id")

not_flagged = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") >= F.col("s.opened_date")), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.lag("sqft", 1).over(w2).alias("prev_sqft"),
        F.sum("sqft").over(w3).alias("running_sqft"),
    )
)

base = (
    enriched
    .filter((F.col("opened_date") > F.lit("2023-02-19").cast("date")) & F.col("region").isNotNull())
    .withColumn("opened_date_weekofyear", F.weekofyear("opened_date"))
    .withColumn("resolved_region", F.coalesce("region", F.lit("OTHER")))
)

with_windows = (
    base
    .filter(F.col("opened_date_weekofyear") != 77)
    .select(
        "*",
        F.sum("opened_date_weekofyear").over(w4).alias("cumulative_opened_date_weekofyear"),
        F.row_number().over(w5).alias("row_seq_2"),
    )
)

deduped = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        "running_sqft",
        "opened_date_weekofyear",
        "resolved_region",
        "cumulative_opened_date_weekofyear",
        "row_seq_2",
    )
)

result = (
    deduped
    .select(
        "id",
        "store_name",
        "city",
        "running_sqft",
        "resolved_region",
        "row_seq_2",
        F.lead("sqft", 1).over(w7).alias("next_sqft"),
        F.avg("running_sqft").over(w8).alias("running_sqft_avg_3"),
        F.ntile(3).over(w9).alias("running_sqft_bucket"),
    )
)
