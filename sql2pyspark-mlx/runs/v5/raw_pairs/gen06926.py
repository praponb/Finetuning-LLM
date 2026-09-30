from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("is_de").asc_nulls_first())

valid = (
    df_suppliers
    .filter(F.col("country") == "JP")
    .withColumn("is_de", F.when(F.col("country") == "DE", 1).otherwise(0))
    .withColumn("city_filled", F.coalesce("city", F.lit("UNKNOWN")))
)

deduped = (
    valid
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

matched_keys = (
    deduped
    .select("city", "id")
    .intersectAll(
        df_suppliers
        .filter(F.col("country").isin(["TH", "US"]))
        .select("city", "id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("rating") >= 2.7)
    .select(
        "supplier_name",
        "city",
        "rating",
        "is_de",
        F.when(F.col("country") == "UK", 1).otherwise(0).alias("is_uk"),
    )
)

result = (
    cleaned
    .select("supplier_name", "city", "rating", "is_uk")
    .orderBy(F.col("rating").asc_nulls_first())
)
