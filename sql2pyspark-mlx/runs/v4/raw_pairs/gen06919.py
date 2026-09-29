from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region").orderBy("sqft", "id")

cleaned = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-07-19"))) & F.col("store_name").like("%Riversi%"))
    .withColumn("sqft_whole", F.ceil("sqft"))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") < F.col("s.opened_date")), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") <= F.col("s.opened_date")), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.order_date"), F.col("c.status").alias("order_status"))
)

latest = (
    latest_match
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .groupBy(F.date_trunc("week", "order_date").alias("week_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
    )
)
