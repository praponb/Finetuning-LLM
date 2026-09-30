from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity_num", "id")

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-08-05")))
    .withColumn("quantity_num", F.col("quantity").cast("double"))
    .withColumn("last_restock_label", F.date_format("last_restock", "yyyy-MM"))
)

enriched = (
    filtered
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.city"), F.col("d.store_name"), F.col("d.opened_date"))
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

base = (
    ranked
    .filter((F.col("last_restock") > F.lit("2023-08-04").cast("date")) & F.col("city").isNotNull())
    .withColumn("quantity_times_quantity_num", (F.col("quantity") * F.col("quantity_num")))
    .withColumn("adjusted_quantity", F.round((F.col("quantity") * 1.07), 2))
)

totals = (
    base
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 217, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["quantity_tier", "product_id"], ["quantity_tier"]],
        "quantity_tier",
        "product_id",
    )
    .agg(
        F.percentile_approx("quantity_num", [0.25, 0.5, 0.75]).alias("quantity_num_quartiles"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.sum("adjusted_quantity").alias("total_adjusted_quantity"),
    )
    .filter(F.col("total_adjusted_quantity") > 0)
)

result = (
    totals
    .select("quantity_tier", "city_list", "new_york_events", "distinct_store_ids")
    .orderBy(F.col("quantity_tier").desc())
)
