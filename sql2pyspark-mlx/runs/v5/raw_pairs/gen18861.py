from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("week_start").orderBy(F.col("fedex_events").desc())

result = (
    df_shipments
    .groupBy(F.date_trunc("WEEK", "delivered_date").alias("week_start"), "status")
    .agg(
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("carrier") == "Flash", 1)), F.lit(0)), 2).alias("cost_per_flash"),
        F.count_if(F.col("carrier") == "Kerry").alias("kerry_events"),
        F.count("*").alias("row_count"),
        F.count_if(F.col("carrier") == "FedEx").alias("fedex_events"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
    .withColumn("fedex_events_rank", F.rank().over(w))
    .orderBy(F.col("fedex_events").desc())
)
