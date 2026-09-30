from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy(F.col("launch_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("city", "category").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("city").orderBy("running_max_cost", "id")
w_ordered_2 = Window.partitionBy("brand", "city").orderBy(F.col("running_avg_cost").desc(), "id")
w6 = Window.partitionBy("category")
w7 = Window.partitionBy("city")

latest = (
    df_products
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

joined = (
    latest
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.supplier_name"), F.col("b.city"), F.col("b.rating"))
)

enriched = (
    joined
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_name",
        "city",
        F.min("cost").over(w2).alias("running_min_cost"),
        F.max("cost").over(w2).alias("running_max_cost"),
        F.avg("cost").over(w2).alias("running_avg_cost"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "running_max_cost",
        "running_avg_cost",
        F.sum("price").over(w_ordered).alias("cumulative_price"),
        F.lag("price", 1).over(w_part).alias("prev_price"),
        F.row_number().over(w_ordered_2).alias("event_seq"),
        F.max("price").over(w6).alias("category_max_price"),
        F.corr("price", "stock_qty").over(w7).alias("corr_price_stock_qty"),
    )
)

result = (
    metrics
    .withColumn("week_start", F.date_trunc("WEEK", "launch_date"))
    .withColumn("running_avg_cost_tier", F.when(F.col("running_avg_cost") >= 810, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["week_start", "running_avg_cost_tier"], ["week_start"]],
        "week_start",
        "running_avg_cost_tier",
    )
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.sum(F.when(F.col("brand") == "Initech", F.col("price")).otherwise(0)).alias("initech_price"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("brand") == "Stark", 1)), F.lit(0)), 2).alias("stock_qty_per_stark"),
    )
    .orderBy(F.col("stock_qty_per_stark").desc())
)
