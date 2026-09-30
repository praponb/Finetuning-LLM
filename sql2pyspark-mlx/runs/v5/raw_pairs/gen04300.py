from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("device")
w3 = Window.partitionBy("web_event_event_type").orderBy(F.col("is_product").desc(), "id")
w4 = Window.partitionBy("country").orderBy("device_min_is_product", "id")
w_grp = Window.orderBy("is_product", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("web_event_event_type").orderBy("is_product_bucket", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("device").orderBy("is_product_bucket", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.partitionBy("browser")

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.browser",
        "s.country",
        F.col("c.page"),
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.page").isin(["/home", "/search"])), "left_anti")
)

cleaned = (
    not_flagged
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "device",
        "browser",
        "country",
        "page",
        "web_event_event_type",
        F.initcap(F.trim("web_event_event_type")).alias("web_event_event_type_title"),
    )
)

valid = (
    cleaned
    .filter(F.col("page").isNotNull())
    .withColumn("is_product", F.when(F.col("page") == "/product", 1).otherwise(0))
)

scored = (
    valid
    .filter(F.col("is_product").between(58, 61))
    .select(
        "*",
        F.min("is_product").over(w2).alias("device_min_is_product"),
        F.ntile(10).over(w3).alias("is_product_bucket"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "country",
        "web_event_event_type",
        "web_event_event_type_title",
        F.lag("is_product", 1).over(w4).alias("prev_is_product"),
        F.avg("is_product_bucket").over(w_grp).alias("is_product_bucket_avg_5_prior"),
        F.avg("is_product").over(w6).alias("is_product_avg_2_prior"),
        F.avg("device_min_is_product").over(w7).alias("device_min_is_product_avg_6"),
        F.round((F.col("is_product_bucket") / F.sum("is_product_bucket").over(w8)), 4).alias("is_product_bucket_share"),
    )
)

daily = (
    enriched
    .groupBy(
        F.when(F.col("device_min_is_product_avg_6") >= 77, "LARGE").otherwise("SMALL").alias("device_min_is_product_avg_6_tier"),
    )
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.size(F.collect_set("web_event_event_type")).alias("num_web_event_event_types"),
        F.round(F.sum("device_min_is_product_avg_6") / F.nullif(F.count(F.when(F.col("web_event_event_type") == "add_to_cart", 1)), F.lit(0)), 2).alias("device_min_is_product_avg_6_per_add_to_cart"),
        F.round(F.sum("device_min_is_product_avg_6") / F.nullif(F.count(F.when(F.col("country") == "US", 1)), F.lit(0)), 2).alias("device_min_is_product_avg_6_per_us"),
        F.count_if(F.col("country") == "JP").alias("jp_events"),
    )
)

result = (
    daily
    .select(
        "device_min_is_product_avg_6_tier",
        "us_count",
        "num_web_event_event_types",
        "device_min_is_product_avg_6_per_add_to_cart",
        "device_min_is_product_avg_6_per_us",
    )
)
