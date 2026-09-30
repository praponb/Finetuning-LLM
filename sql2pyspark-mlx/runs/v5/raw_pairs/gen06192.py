from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w4 = Window.orderBy("product_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("product_cost").desc(), "id")
w7 = Window.orderBy("running_sum_rating", "id").rowsBetween(-3, Window.currentRow)

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date").between("2023-12-01", "2024-08-15")), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.brand") == "Acme"), "left_semi")
)

enriched = (
    kept
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.min("rating").over(w_hist).alias("running_min_rating"),
        F.sum("rating").over(w_hist).alias("running_sum_rating"),
        F.max("rating").over(w_hist).alias("running_max_rating"),
        F.avg("rating").over(w_grp).alias("rating_avg_6"),
    )
)

filtered = (
    enriched
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "city",
        "running_min_rating",
        "running_sum_rating",
        "running_max_rating",
        F.initcap(F.trim("city")).alias("city_title"),
        (F.col("running_sum_rating") - F.col("rating_avg_6")).alias("running_sum_rating_minus_rating_avg_6"),
    )
)

latest_match = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.running_sum_rating",
        "s.running_sum_rating_minus_rating_avg_6",
        F.col("c.cost").alias("product_cost"),
        F.col("c.price"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (~F.col("ch.category").isin(["Fashion", "Electronics"])), "left_anti")
)

enriched_suppliers = (
    not_flagged
    .select(
        "*",
        F.sum("product_cost").over(w4).alias("cumulative_product_cost"),
        F.sum("running_sum_rating").over(w5).alias("cumulative_running_sum_rating"),
        F.row_number().over(w6).alias("event_seq"),
        F.avg("running_sum_rating").over(w7).alias("running_sum_rating_avg_3"),
    )
)

result = (
    enriched_suppliers
    .groupBy(
        F.when(F.col("product_cost") >= 720, "HIGH").otherwise("LOW").alias("product_cost_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("cumulative_product_cost", 0.5).alias("cumulative_product_cost_p50"),
        F.round(F.avg("cumulative_running_sum_rating"), 2).alias("avg_cumulative_running_sum_rating"),
        F.round(F.avg("price"), 2).alias("avg_price"),
    )
    .orderBy("cumulative_product_cost_p50")
)
