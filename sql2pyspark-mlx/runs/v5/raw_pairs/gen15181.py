from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "brand").orderBy(F.col("launch_date").desc(), "id")
w2 = Window.partitionBy("supplier_id", "brand").orderBy("launch_date", "id")
w3 = Window.partitionBy("brand").orderBy("cost", "id")
w4 = Window.partitionBy("supplier_id")
w5 = Window.partitionBy("brand").orderBy(F.col("rank_in_group").desc(), "id")

filtered = (
    df_products
    .filter((F.col("stock_qty") <= 397) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "launch_date",
        "supplier_id",
        F.concat("category", F.lit("/"), "product_name").alias("category_product_name"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_filled"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

latest_products = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    latest_products
    .select(
        "id",
        "brand",
        "cost",
        "supplier_id",
        "brand_filled",
        "rank_in_group",
        F.lead("cost").over(w3).alias("next_cost"),
        F.lag("cost").over(w3).alias("prev_cost"),
        F.row_number().over(w3).alias("event_seq"),
        F.round((F.col("cost") / F.sum("cost").over(w4)), 4).alias("cost_share"),
    )
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

stats = (
    ranked
    .withColumn("cost_share_tier", F.when(F.col("cost_share") >= 0.65, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["supplier_id", "brand", "cost_share_tier"], ["supplier_id", "brand"], []],
        "supplier_id",
        "brand",
        "cost_share_tier",
    )
    .agg(
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.round(F.sum("cost_share") / F.nullif(F.count(F.when(F.col("brand") == "Umbrella", 1)), F.lit(0)), 2).alias("cost_share_per_umbrella"),
    )
)

result = (
    stats
    .select(
        "brand",
        "cost_share_tier",
        "cost_share_per_umbrella",
        F.when(F.col("brand").rlike("^(Umbrella|Initech)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("brand_zone"),
    )
    .orderBy(F.col("cost_share_tier").desc())
)
