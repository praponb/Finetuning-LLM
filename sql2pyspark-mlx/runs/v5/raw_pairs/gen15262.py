from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city").orderBy("opened_date", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("region").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region", "status").orderBy(F.col("next_sqft").desc(), "id")

enriched = (
    df_stores
    .filter(F.col("opened_date") < "2024-09-01")
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        F.lead("sqft").over(w_running).alias("next_sqft"),
        F.lag("sqft").over(w_running).alias("prev_sqft"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.opened_date",
        "s.next_sqft",
        F.col("c.status"),
        F.col("c.shipping_fee"),
    )
)

with_windows = (
    with_latest
    .withColumn("next_sqft_avg_2_prior", F.avg("next_sqft").over(w_ordered))
    .withColumn("last_known_shipping_fee", F.last("shipping_fee", ignorenulls=True).over(w4))
    .withColumn("next_sqft_bucket", F.ntile(10).over(w5))
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") <= F.col("s.opened_date")), "left_semi")
)

result = (
    not_flagged
    .withColumn("week_start", F.date_trunc("week", "opened_date"))
    .groupingSets([["region", "week_start"], ["region"]], "region", "week_start")
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.avg(F.when(F.col("region") == "West", F.col("shipping_fee"))).alias("west_avg_shipping_fee"),
        F.count("*").alias("cnt"),
        F.round(F.avg("next_sqft_avg_2_prior"), 2).alias("avg_next_sqft_avg_2_prior"),
        F.grouping("region").alias("grouping_level"),
    )
    .orderBy(F.col("week_start").desc())
)
