from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id")
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("sqft", "id")
w4 = Window.partitionBy("city").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city", "region")

valid = (
    df_stores
    .filter(F.col("opened_date") >= "2025-05-24")
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.substring("region", 1, 4).alias("region_prefix"),
        F.concat("store_name", F.lit("/"), "city").alias("store_name_city"),
        F.coalesce("region", F.lit("NONE")).alias("resolved_region"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.last("sqft", ignorenulls=True).over(w2).alias("last_known_sqft"),
        F.lead("sqft", 1, 0).over(w_part).alias("next_sqft"),
    )
)

metrics = (
    scored
    .withColumn("last_known_next_sqft", F.last("next_sqft", ignorenulls=True).over(w4))
    .withColumn("event_seq_share", F.round((F.col("event_seq") / F.sum("event_seq").over(w5)), 4))
)

result = (
    metrics
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "region_prefix",
        "store_name_city",
        "resolved_region",
        "event_seq",
        "last_known_sqft",
        F.when(F.col("city").rlike("^(Tokyo|Singapore)$"), "CORE").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_A").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("city").desc_nulls_last())
)
