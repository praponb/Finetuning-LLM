from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc_nulls_first())
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("country")

kept_keys = (
    df_suppliers
    .select("id", "city")
    .subtract(
        df_suppliers
        .filter(F.col("city").isin(["Singapore", "Bangkok", "Berlin"]))
        .select("id", "city")
    )
)

kept_rows = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.brand").isin(["Acme", "Globex"])), "left_anti")
)

with_windows = (
    eligible
    .withColumn("rating_cume_dist", F.cume_dist().over(w))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w2)), 4))
    .withColumn("country_min_rating", F.min("rating").over(w3))
)

kept = (
    with_windows
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.price") < 410), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "rating_cume_dist",
        "rating_share",
        "country_min_rating",
        F.when(F.col("country").rlike("^(SG|US)$"), "PRIORITY").when(F.col("country").rlike("^(TH|JP)$"), "GROUP_A").otherwise("GROUP_B").alias("country_zone"),
    )
    .orderBy(F.col("country").desc_nulls_last())
)
