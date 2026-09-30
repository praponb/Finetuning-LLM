from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("sqft") < 7666)
    .withColumn("city_upper", F.upper("city"))
)

not_flagged = (
    base
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.opened_date")), "left_anti")
)

filtered = (
    not_flagged
    .filter((F.col("opened_date") >= F.lit("2023-06-24").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "city_upper",
        F.regexp_replace("store_name", " ", "-").alias("store_name_slug"),
        F.when(F.col("city").isin(["New York", "Singapore"]), "Tier 1").otherwise("Other").alias("city_group"),
        F.coalesce("region", F.lit("OTHER")).alias("resolved_region"),
    )
)

valid = (
    filtered
    .filter((F.col("opened_date") >= F.lit("2023-07-26").cast("date")) & F.col("region").isNotNull() & (F.col("region") != "West"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "city_upper",
        "store_name_slug",
        "city_group",
        F.substring("city", 1, 2).alias("city_prefix"),
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_clean"),
    )
)

grouped = (
    valid
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("region") == "West", F.col("days_since_opened_date"))).alias("west_total_days_since_opened_date"),
        F.avg("days_since_opened_date").alias("avg_days_since_opened_date"),
        F.round(F.sum("days_since_opened_date") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("days_since_opened_date_per_new_york"),
    )
    .filter(F.col("west_total_days_since_opened_date") > 0)
)

result = (
    grouped
    .select("city", "avg_days_since_opened_date", "days_since_opened_date_per_new_york")
    .orderBy(F.col("days_since_opened_date_per_new_york").desc_nulls_last(), "city")
    .limit(10)
)
