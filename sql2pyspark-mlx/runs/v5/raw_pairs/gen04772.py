from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")
w2 = Window.partitionBy("product_id").orderBy("prev_sqft", "id")

enriched = (
    df_store_sales
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.sqft"), F.col("p.city"))
)

cleaned = (
    enriched
    .filter((F.col("sale_date") > "2025-05-01") & F.col("city").isNotNull())
    .select("*", F.date_format("sale_date", "MMM yyyy").alias("sale_date_label"))
)

enriched_store_sales = (
    cleaned
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "sqft",
        "sale_date_label",
        F.lead("revenue").over(w).alias("next_revenue"),
        F.lag("revenue").over(w).alias("prev_revenue"),
        F.lag("sqft").over(w).alias("prev_sqft"),
        F.row_number().over(w).alias("seq_no"),
    )
)

ranked = (
    enriched_store_sales
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "units", "sale_date_label", "seq_no")
)

matched_keys = (
    ranked
    .select("id")
    .intersect(
        df_store_sales
        .filter(F.col("units") != 29)
        .select("id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("units") >= 42, "LARGE").otherwise("SMALL").alias("units_tier"),
    )
    .agg(
        F.avg("units").alias("avg_units"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
    )
    .orderBy("avg_seq_no")
)
