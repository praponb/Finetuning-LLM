from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

filtered = (
    df_suppliers
    .filter(F.col("city") == "Singapore")
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

valid = (
    filtered
    .filter(F.col("country") == "DE")
    .select(
        "id",
        "country",
        "city",
        F.concat("city", F.lit(" "), "country").alias("city_country"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled_2"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price"))
)

result = (
    latest_match
    .select("id", "country", "city_country", "city_filled_2", "price")
    .orderBy(F.col("country").desc_nulls_last())
)
