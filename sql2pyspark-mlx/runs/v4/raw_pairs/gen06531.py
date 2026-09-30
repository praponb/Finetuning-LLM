from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region").orderBy(F.col("opened_date").desc_nulls_first())
w3 = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")

latest_match = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"))
)

deduped = (
    latest_match
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

kept = (
    latest
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") >= F.col("s.opened_date")), "left_semi")
)

grouped = (
    kept
    .withColumn("amount_tier", F.when(F.col("amount") >= 1630, "LARGE").otherwise("SMALL"))
    .cube("amount_tier")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    grouped
    .select("amount_tier", "cnt", "distinct_ids", "num_regions")
    .orderBy(F.col("num_regions").desc())
)
