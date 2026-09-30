from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy("rating", "id")
w3 = Window.partitionBy("country")
w4 = Window.partitionBy("country", "city")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("country").orderBy(F.col("next_rating").desc_nulls_last(), "id")

cleaned = (
    df_suppliers
    .filter((F.col("city").eqNullSafe(F.lit("Tokyo"))) & F.col("rating").isNotNull())
    .select(
        "*",
        F.lower("country").alias("country_lower"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "country",
        "city",
        "country_lower",
        "city_clean",
        F.lead("rating", 2).over(w).alias("next_rating"),
        F.ntile(4).over(w2).alias("rating_bucket"),
        F.lead("rating", 1).over(w2).alias("next_rating_2"),
        F.count("rating").over(w3).alias("country_count_rating"),
        F.sum("rating").over(w4).alias("country_sum_rating"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date").alias("product_launch_date"))
)

ranked = (
    with_last
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select(
        "id",
        "country",
        "city",
        "city_clean",
        "next_rating",
        "rating_bucket",
        "next_rating_2",
        "country_count_rating",
        "product_launch_date",
    )
    .orderBy(F.col("product_launch_date").desc())
)
