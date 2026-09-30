from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").asc_nulls_first(), "id")
w2 = Window.partitionBy("city", "country").orderBy("rating", "id")
w3 = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

deduped = (
    df_suppliers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

ranked = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

latest = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

not_flagged = (
    latest
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id").isNull()), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w4))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.category"))
)

kept_keys = (
    latest_match
    .select("country", "id")
    .intersectAll(
        df_suppliers
        .filter(F.col("city") == "Chiang Mai")
        .select("country", "id")
    )
)

verified = (
    latest_match
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id") == 39), "left_semi")
)

result = (
    eligible
    .select("supplier_name", "country", "city", "latest_rank")
    .orderBy(F.col("city").desc())
)
