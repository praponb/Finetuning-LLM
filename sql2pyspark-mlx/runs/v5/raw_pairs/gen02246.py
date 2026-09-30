from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy(F.col("cost").desc())
w_recent = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("category").orderBy("cost", "id")

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.col("supplier_id").isin([24, 39]))
    .select(
        "*",
        F.last_day("launch_date").alias("launch_date_month_end"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.quantity").isNotNull()), "left_semi")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.quantity").isNull()), "left_semi")
)

deduped = (
    eligible
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "resolved_brand",
    )
)

enriched = (
    deduped
    .filter(F.length("product_name") > 12)
    .select(
        "id",
        "product_name",
        "launch_date",
        F.avg("cost").over(w_recent).alias("running_avg_cost"),
        F.min("cost").over(w_recent).alias("running_min_cost"),
        F.count("cost").over(w_recent).alias("running_count_cost"),
        F.sum("cost").over(w_recent).alias("running_sum_cost"),
        F.lead("cost", 2).over(w3).alias("next_cost"),
    )
)

result = (
    enriched
    .withColumn("quarter_start", F.date_trunc("QUARTER", "launch_date"))
    .withColumn("activity_month", F.date_trunc("MONTH", "launch_date"))
    .groupingSets(
        [["quarter_start", "activity_month"], ["quarter_start"], []],
        "quarter_start",
        "activity_month",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.countDistinct("id").alias("distinct_ids_3"),
        F.grouping("quarter_start").alias("grouping_level"),
    )
)
