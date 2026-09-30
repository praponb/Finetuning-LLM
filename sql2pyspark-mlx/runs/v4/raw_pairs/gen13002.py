from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").asc_nulls_last())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("supplier_name").like("%EuroGoo%"))
    .withColumn("city_filled", F.coalesce("city", F.lit("N/A")))
    .withColumn("country_prefix", F.substring("country", 1, 4))
)

base = (
    valid
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "city_filled",
        "country_prefix",
        F.trim("city").alias("clean_city"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_latest = (
    ranked
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category").alias("product_category"))
)

result = (
    with_latest
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "country_prefix",
        "clean_city",
        "latest_rank",
        F.when(F.col("supplier_name").rlike("^(Global Supply|EuroGoods GmbH)$"), "OTHER_ZONE").when(F.col("supplier_name").rlike("^(Siam Parts Co|Tokyo Traders)$"), "EXTENDED").otherwise("GROUP_B").alias("supplier_name_zone"),
    )
    .orderBy(F.col("latest_rank").desc_nulls_first())
)
