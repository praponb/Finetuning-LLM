from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy(F.col("sqft").desc_nulls_first())

valid = (
    df_stores
    .filter(F.year("opened_date") == 2023)
    .withColumn("sqft_str", F.col("sqft").cast("double"))
    .withColumn("region_filled", F.coalesce("region", F.lit("DIRECT")))
)

kept = (
    valid
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id").isin([1, 3])), "left_anti")
)

cleaned = (
    kept
    .filter((F.col("opened_date") > F.lit("2023-06-15").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.concat("store_name", F.lit(" "), "region").alias("store_name_region"),
        F.weekofyear("opened_date").alias("opened_date_weekofyear"),
    )
)

cleaned_stores = (
    cleaned
    .filter((F.col("opened_date") > F.lit("2023-09-11").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "region",
        "sqft",
        "sqft_str",
        "region_filled",
        "store_name_region",
        "opened_date_weekofyear",
        F.substring("store_name", 1, 3).alias("store_name_prefix"),
    )
)

with_windows = (
    cleaned_stores
    .filter(F.col("sqft") >= 10182)
    .select(
        "*",
        F.avg("opened_date_weekofyear").over(w).alias("opened_date_weekofyear_avg_3"),
        F.dense_rank().over(w2).alias("sqft_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "store_name_prefix",
        "opened_date_weekofyear_avg_3",
        "sqft_dense_rank",
    )
)
