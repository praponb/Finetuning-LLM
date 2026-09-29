from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("key").orderBy("created_at", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("key").orderBy(F.col("created_at").desc(), "id", "key")

exploded = (
    df_carts
    .select("id", "created_at", F.explode("attributes").alias("key", "attr_value"))
)

scored = (
    exploded
    .select(
        "*",
        F.first("key", ignorenulls=True).over(w_running).alias("first_known_key"),
        F.last("key", ignorenulls=True).over(w_running).alias("last_known_key"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

matched_keys = (
    latest
    .select("id")
    .subtract(
        df_carts
        .filter(F.size("coupon_codes") >= 1)
        .select("id")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "key",
        "attr_value",
        "first_known_key",
        F.when(F.col("key").rlike("^(weight_kg|device)$"), "CORE").when(F.col("key").rlike("^(utm_source|gift)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("key_zone"),
    )
    .orderBy(F.col("id").desc())
)
