from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_first(), "id")

not_flagged = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") < F.col("s.opened_date")), "left_anti")
)

scored = (
    not_flagged
    .filter(F.col("opened_date") >= "2024-05-01")
    .select(
        "*",
        F.avg("sqft").over(w).alias("sqft_avg_3"),
        F.count("sqft").over(w2).alias("city_count_sqft"),
    )
)

latest = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "store_name", "sqft", "sqft_avg_3", "city_count_sqft")
)

kept = (
    latest
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.amount") < 1250), "left_semi")
)

filtered = (
    kept
    .filter(F.col("sqft_avg_3").isNotNull())
    .select(
        "id",
        "store_name",
        "sqft",
        "sqft_avg_3",
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
    )
)

result = (
    filtered
    .select("id", "store_name", "sqft", "store_name_normalized")
    .orderBy(F.col("id").desc_nulls_last())
)
