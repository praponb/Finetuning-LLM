from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc())
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w3 = Window.partitionBy("city").orderBy(F.col("rating_percent_rank").desc_nulls_first())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("product_category").orderBy(F.col("rating_percent_rank").desc_nulls_last(), "id")
w6 = Window.partitionBy("product_category", "city")
w_grp = Window.partitionBy("city").orderBy(F.col("price").desc())
w8 = Window.partitionBy("product_category", "city").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_category", "city").orderBy("rating_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("price", "id")

enriched = (
    df_suppliers
    .select(
        "id",
        "city",
        F.percent_rank().over(w).alias("rating_percent_rank"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "city", "rating_percent_rank", "row_seq")
)

kept = (
    ranked
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.brand").isin(["Initech", "Stark"])), "left_anti")
)

cleaned = (
    kept
    .filter(F.col("city") != "London")
    .select(
        "*",
        F.lower("city").alias("city_lower"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.rating_percent_rank",
        "s.city_lower",
        F.col("c.category").alias("product_category"),
        F.col("c.price"),
    )
)

latest = (
    latest_match
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows = (
    latest
    .select(
        "id",
        "city",
        "rating_percent_rank",
        "city_lower",
        F.round((F.col("price") / F.sum("price").over(w6)), 4).alias("price_share"),
        F.dense_rank().over(w_grp).alias("price_dense_rank"),
        F.sum("rating_percent_rank").over(w8).alias("running_rating_percent_rank"),
        F.last("product_category", ignorenulls=True).over(w_recent).alias("last_known_product_category"),
        F.lead("price", 1).over(w10).alias("next_price"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "city",
        "city_lower",
        "price_share",
        "running_rating_percent_rank",
        "last_known_product_category",
        "next_price",
    )
    .orderBy(F.col("running_rating_percent_rank").desc_nulls_first())
)
