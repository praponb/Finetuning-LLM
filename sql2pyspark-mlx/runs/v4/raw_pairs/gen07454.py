from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating_scaled").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("clean_country", F.trim("country"))
    .withColumn("rating_scaled", F.round((F.col("rating") * 0.9), 2))
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

base = (
    latest
    .filter(F.col("city").isNotNull() & (F.col("rating_scaled") < 44))
    .select(
        "*",
        F.when(F.col("city") == "Chiang Mai", 1).otherwise(0).alias("is_chiang_mai"),
        F.greatest("rating", "rating_scaled").alias("greatest_rating_rating_scaled"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_clean"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price"))
)

result = (
    with_last
    .select(
        "id",
        "country",
        "city",
        "clean_country",
        "rating_scaled",
        "is_chiang_mai",
        "greatest_rating_rating_scaled",
        "city_clean",
        "price",
        F.when(F.col("city").rlike("^(Tokyo|Singapore)$"), "PRIORITY").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy(F.col("price").desc_nulls_last())
)
