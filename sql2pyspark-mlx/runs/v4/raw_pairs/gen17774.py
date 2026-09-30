from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

base = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-08-08"))) & F.col("region").isNotNull())
    .select(
        "*",
        F.concat("city", F.lit(" - "), "store_name").alias("city_store_name"),
        F.trim("city").alias("clean_city"),
        F.coalesce("region", F.lit("N/A")).alias("region_clean"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") <= F.col("s.opened_date")), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"), F.col("c.last_restock"))
)

matched_keys = (
    latest_match
    .select("region", "id")
    .subtract(
        df_stores
        .filter(~F.col("city").isin(["Tokyo", "Chiang Mai"]))
        .select("region", "id")
    )
)

kept_rows = (
    latest_match
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

summary = (
    kept_rows
    .cube(F.date_trunc("quarter", "opened_date").alias("quarter_start"), "city")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.sum("sqft").alias("total_sqft"),
        F.avg(F.when(F.col("city") == "Tokyo", F.col("quantity"))).alias("tokyo_avg_quantity"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "South", 1)), F.lit(0)), 2).alias("sqft_per_south"),
    )
)

result = (
    summary
    .select(
        "city",
        "avg_sqft",
        "total_sqft",
        "tokyo_avg_quantity",
        "sqft_bit_and",
        "sqft_per_south",
        F.when(F.col("city").rlike("^(New York|London)$"), "PRIORITY").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("sqft_bit_and").desc_nulls_last())
)
