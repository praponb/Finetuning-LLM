from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc_nulls_last(), "id")
w_grp = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("delivered_date_dayofweek", "id")
w4 = Window.partitionBy("carrier", "status").orderBy("delivered_date_dayofweek", "id")
w5 = Window.orderBy("running_count_delivered_date_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("running_running_count_delivered_date_dayofweek", "id").rowsBetween(-5, -1)
w7 = Window.partitionBy("carrier").orderBy("running_avg_delivered_date_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("carrier").orderBy("delivered_date_dayofweek_bucket", "id").rowsBetween(-6, -1)
w9 = Window.partitionBy("carrier").orderBy(F.col("running_avg_delivered_date_dayofweek").desc(), "id")
w10 = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc_nulls_last())

base = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .withColumn("delivered_date_dayofweek", F.dayofweek("delivered_date"))
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "carrier", "delivered_date", "status", "delivered_date_dayofweek")
)

metrics = (
    deduped
    .select(
        "*",
        F.count("delivered_date_dayofweek").over(w_grp).alias("running_count_delivered_date_dayofweek"),
        F.min("delivered_date_dayofweek").over(w_grp).alias("running_min_delivered_date_dayofweek"),
        F.avg("delivered_date_dayofweek").over(w_grp).alias("running_avg_delivered_date_dayofweek"),
    )
)

metrics_shipments = (
    metrics
    .select(
        "id",
        "carrier",
        "delivered_date",
        "delivered_date_dayofweek",
        "running_min_delivered_date_dayofweek",
        "running_avg_delivered_date_dayofweek",
        F.ntile(5).over(w3).alias("delivered_date_dayofweek_bucket"),
        F.row_number().over(w4).alias("row_seq"),
        F.sum("running_count_delivered_date_dayofweek").over(w5).alias("running_running_count_delivered_date_dayofweek"),
    )
)

with_windows = (
    metrics_shipments
    .select(
        "*",
        F.avg("running_avg_delivered_date_dayofweek").over(w6).alias("running_avg_delivered_date_dayofweek_avg_5_prior"),
        F.sum("delivered_date_dayofweek").over(w7).alias("running_delivered_date_dayofweek"),
        F.sum("running_avg_delivered_date_dayofweek").over(w7).alias("running_running_avg_delivered_date_dayofweek"),
        F.avg("row_seq").over(w8).alias("row_seq_avg_6_prior"),
        F.row_number().over(w9).alias("seq_no"),
    )
)

deduped_shipments = (
    with_windows
    .withColumn("rank_in_group", F.rank().over(w10))
    .filter(F.col("rank_in_group") == 1)
)

cleaned = (
    deduped_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("running_min_delivered_date_dayofweek") < 66))
    .select(
        "*",
        F.round((F.col("running_avg_delivered_date_dayofweek") / F.col("seq_no")), 2).alias("running_avg_delivered_date_dayofweek_per_seq_no"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_filled"),
    )
)

result = (
    cleaned
    .groupBy("carrier")
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.when(F.col("carrier") == "DHL", F.col("running_avg_delivered_date_dayofweek_per_seq_no"))).alias("dhl_max_running_avg_delivered_date_dayofweek_per_seq_no"),
    )
    .orderBy(F.col("cnt").asc_nulls_last())
)
