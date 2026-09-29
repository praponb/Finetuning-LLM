from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("city", "region").orderBy(F.col("is_bangkok").desc(), "id")
w3 = Window.partitionBy("city", "region").orderBy("opened_date", "id")

valid = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("city") == "Berlin"))
    .withColumn("is_bangkok", F.when(F.col("city") == "Bangkok", 1).otherwise(0))
    .withColumn("clean_store_name", F.trim("store_name"))
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

eligible = (
    ranked
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date").between("2023-11-01", "2024-05-01")), "left_anti")
)

latest = (
    eligible
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

deduped = (
    latest
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

kept_keys = (
    deduped
    .select("city", "region")
    .subtract(
        df_stores
        .filter(~F.col("store_name").like("D%"))
        .select("city", "region")
    )
)

verified = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "is_bangkok",
        "clean_store_name",
        "dedup_rank",
        "latest_rank",
    )
    .orderBy(F.col("id").asc_nulls_last())
)
