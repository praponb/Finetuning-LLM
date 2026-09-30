from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")
w2 = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")
w4 = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc_nulls_first())

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2024-03-08"))) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.concat_ws("/", "store_name", "city").alias("store_name_city_key"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.lag("sqft", 2, 0).over(w).alias("prev_sqft"),
        F.ntile(4).over(w2).alias("sqft_bucket"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.product_id").isin([2, 3, 9, 10])), "left_semi")
)

deduped = (
    not_flagged
    .withColumn("rn", F.rank().over(w4))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        "resolved_region",
        "prev_sqft",
        "rank_in_group",
        "rn",
        F.when(F.col("city").rlike("^(Chiang Mai|New York)$"), "GROUP_A").when(F.col("city").rlike("^(Bangkok|Singapore)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("city_zone"),
    )
    .orderBy(F.col("sqft").desc())
)
