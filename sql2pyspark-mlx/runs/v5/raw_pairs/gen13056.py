from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.orderBy("sale_date", "id")
w3 = Window.orderBy(F.col("sqft").desc_nulls_first())
w4 = Window.partitionBy("store_id")
w5 = Window.partitionBy("region", "store_id").orderBy("prev_sqft", "id")
w6 = Window.partitionBy("region").orderBy("prev_sqft")

with_dims = (
    df_store_sales
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.sale_date",
        F.col("p.region"),
        F.col("p.sqft"),
    )
)

scored = (
    with_dims
    .filter(F.year("sale_date") == 2025)
    .withColumn("region_stddev_samp_sqft", F.stddev_samp("sqft").over(w))
    .withColumn("prev_sqft", F.lag("sqft", 2).over(w2))
    .withColumn("sqft_rank", F.rank().over(w3))
)

with_windows = (
    scored
    .select(
        "id",
        "sale_date",
        "region",
        "prev_sqft",
        "sqft_rank",
        F.round((F.col("region_stddev_samp_sqft") / F.sum("region_stddev_samp_sqft").over(w4)), 4).alias("region_stddev_samp_sqft_share"),
        F.lead("prev_sqft", 2).over(w5).alias("next_prev_sqft"),
    )
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.rank().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "sale_date",
        "region",
        "prev_sqft",
        "region_stddev_samp_sqft_share",
        "next_prev_sqft",
        "rank_in_group",
    )
)
