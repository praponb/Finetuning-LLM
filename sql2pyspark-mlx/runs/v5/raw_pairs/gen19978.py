from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date_plus_19d").asc_nulls_last(), "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("delivered_date_plus_19d").desc(), "id")
w3 = Window.partitionBy("carrier").orderBy("latest_rank", "id")
w4 = Window.partitionBy("carrier").orderBy(F.col("latest_rank").desc())

cleaned = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.date_add("delivered_date", 19).alias("delivered_date_plus_19d"),
    )
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "carrier", "delivered_date_plus_19d")
)

deduped = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

ranked_shipments = (
    deduped
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

ranked_shipments_2 = (
    ranked_shipments
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked_shipments_2
    .groupBy(
        "carrier",
        F.date_trunc("QUARTER", "delivered_date_plus_19d").alias("quarter_start"),
    )
    .agg(
        F.countDistinct("carrier").alias("distinct_carriers"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.count("*").alias("row_count"),
        F.size(F.collect_set("carrier")).alias("num_carriers_2"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list_2"),
    )
    .filter(F.col("num_carriers") > 1)
    .orderBy(F.col("quarter_start").desc_nulls_first())
)
