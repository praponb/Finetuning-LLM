from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating_whole").desc_nulls_first(), "id")
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("country").orderBy("rating_whole", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("city")
w6 = Window.orderBy("rating_minus_rating_whole", "id")

valid = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "US"]))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.floor("rating").alias("rating_whole"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.min("rating").over(w2).alias("country_min_rating"),
        F.avg("rating_whole").over(w3).alias("rating_whole_avg_6"),
    )
)

latest_match = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.cost").alias("product_cost"),
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

filtered = (
    latest_match
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        (F.col("rating") - F.col("rating_whole")).alias("rating_minus_rating_whole"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.stock_qty") <= 326), "left_anti")
)

valid_suppliers = (
    kept
    .filter(F.col("rating_whole") < 33)
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "rating_whole",
        "city_filled",
        "row_num",
        "country_min_rating",
        "rating_whole_avg_6",
        "product_cost",
        "product_stock_qty",
        "rating_minus_rating_whole",
        (F.col("rating_minus_rating_whole") - F.col("product_stock_qty")).alias("rating_minus_rating_whole_minus_product_stock_qty"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled_2"),
    )
)

result = (
    valid_suppliers
    .select(
        "*",
        F.min("country_min_rating").over(w5).alias("city_min_country_min_rating"),
        F.lead("rating_whole_avg_6", 1).over(w6).alias("next_rating_whole_avg_6"),
    )
)
