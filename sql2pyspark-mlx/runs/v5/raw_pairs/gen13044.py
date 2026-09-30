from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category", "brand").orderBy(F.col("rating").desc_nulls_first())
w3 = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first())
w4 = Window.partitionBy("country")
w_grp = Window.partitionBy("country", "brand")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.concat_ws(",", "city", "supplier_name").alias("city_supplier_name_key"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category"), F.col("c.brand"))
)

matched_keys = (
    with_latest
    .select("city", "country")
    .subtract(
        df_suppliers
        .filter(~F.col("country").isin(["TH", "UK"]))
        .select("city", "country")
    )
)

reconciled = (
    with_latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .filter(F.col("category").isNotNull())
    .select(
        "country",
        "city_supplier_name_key",
        "brand",
        F.rank().over(w2).alias("rating_rank"),
        F.rank().over(w3).alias("rating_rank_2"),
        F.avg("rating").over(w4).alias("country_avg_rating"),
    )
)

result = (
    enriched
    .select(
        "brand",
        "rating_rank",
        "rating_rank_2",
        F.stddev_samp("rating_rank_2").over(w_grp).alias("country_stddev_samp_rating_rank_2"),
    )
)
