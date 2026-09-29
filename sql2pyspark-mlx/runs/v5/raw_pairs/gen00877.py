from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue_per_unit", "id")
w_recent = Window.partitionBy("city", "region").orderBy("sqft", "id")
w3 = Window.partitionBy("region", "store_id").orderBy(F.col("units").desc(), "id")

filtered = (
    df_store_sales
    .filter(F.col("store_id") == 3)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit"),
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit_2"),
    )
)

joined = (
    filtered
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.sqft") > 14647)
    .select("e.*", F.col("s.city"), F.col("s.sqft"), F.col("s.region"))
)

ranked = (
    joined
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    ranked
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue_per_unit",
        "revenue_per_unit_2",
        "sqft",
        "region",
        F.lag("revenue_per_unit_2").over(w_recent).alias("prev_revenue_per_unit_2"),
        F.lead("revenue_per_unit_2").over(w_recent).alias("next_revenue_per_unit_2"),
        F.lead("units").over(w_recent).alias("next_units"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .cube(F.date_trunc("QUARTER", "sale_date").alias("quarter_start"))
    .agg(
        F.bit_and("units").alias("units_bit_and"),
        F.sum(F.when(F.col("region") == "East", F.col("revenue_per_unit_2"))).alias("east_total_revenue_per_unit_2"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.count_if(F.col("region") == "North").alias("north_events"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
