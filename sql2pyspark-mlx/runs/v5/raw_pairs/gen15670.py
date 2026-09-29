from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("brand").orderBy("cost", "id")
w3 = Window.partitionBy("brand").orderBy(F.col("prev_price").desc_nulls_last(), "id")

base = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-07-19"))) & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "launch_date",
        (F.col("price") - F.col("cost")).alias("margin"),
        F.coalesce("brand", F.lit("DIRECT")).alias("resolved_brand"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.brand",
        "s.price",
        "s.cost",
        "s.launch_date",
        F.col("c.last_restock"),
    )
)

scored = (
    with_last
    .select(
        "id",
        "brand",
        "price",
        "cost",
        "launch_date",
        "last_restock",
        F.lead("cost").over(w2).alias("next_cost"),
        F.lead("price").over(w2).alias("next_price"),
        F.lag("price").over(w2).alias("prev_price"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

matched_keys = (
    deduped
    .select("brand")
    .intersectAll(
        df_products
        .filter(F.upper(F.trim("product_name")) == "COFFEE MAKER")
        .select("brand")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("prev_price") > 350)
    .select(
        "brand",
        "price",
        "cost",
        "launch_date",
        "last_restock",
        "next_cost",
        "next_price",
        "prev_price",
        "seq_no",
        "latest_rank",
        F.round((F.col("seq_no") * 1.1), 2).alias("seq_no_scaled"),
        F.coalesce("brand", F.lit("OTHER")).alias("resolved_brand"),
    )
)

result = (
    cleaned
    .groupBy(
        "brand",
        F.when(F.col("price") >= 930, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.stddev_samp("cost").alias("cost_stddev"),
        F.count_if(F.col("brand") == "Initech").alias("initech_events"),
        F.percentile_approx("next_price", 0.95).alias("next_price_p95"),
    )
    .filter(F.col("initech_events") > 1)
)
