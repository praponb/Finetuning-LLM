from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city", "category").orderBy(F.col("stock_qty").desc(), "id")

base = (
    df_suppliers
    .filter(F.length("supplier_name") > 12)
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.round((F.col("rating") * 1.1), 2).alias("rating_adjusted"),
        F.concat("supplier_name", F.lit(" "), "country").alias("supplier_name_country"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category"), F.col("c.stock_qty"))
)

base_suppliers = (
    with_last
    .filter(F.col("rating_adjusted") < 25)
    .withColumn("stock_qty_times_rating", (F.col("stock_qty") * F.col("rating")))
    .withColumn("resolved_category", F.coalesce("category", F.lit("DIRECT")))
)

latest = (
    base_suppliers
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

kept = (
    latest
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.price") < 720), "left_semi")
)

cleaned = (
    kept
    .filter(F.col("rating_adjusted") <= 38)
    .select(
        "id",
        "country",
        "city",
        "rating",
        "rating_adjusted",
        "category",
        "stock_qty",
        "resolved_category",
        F.concat("country", F.lit("/"), "city").alias("country_city"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "country",
        "city",
        "rating_adjusted",
        "category",
        "stock_qty",
        "country_city",
        F.when(F.col("city").rlike("^(Chiang Mai|Tokyo)$"), "PRIORITY").otherwise("EXTENDED").alias("city_zone"),
    )
)
