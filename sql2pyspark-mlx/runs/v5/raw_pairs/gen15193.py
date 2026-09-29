from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("brand").orderBy("launch_date", "id")
w3 = Window.partitionBy("supplier_id", "category").orderBy("stock_qty", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("category")
w_hist = Window.orderBy("category_avg_stock_qty", "id")
w6 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("cost", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy("brand").orderBy(F.col("cost_avg_6").desc_nulls_first())
w9 = Window.partitionBy("brand").orderBy("cost_avg_6_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("category").orderBy("first_known_cost_avg_3", "id").rowsBetween(-2, Window.currentRow)
w11 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("country").orderBy("seq_no", "id")
w13 = Window.partitionBy("supplier_id")
w14 = Window.partitionBy("supplier_id").orderBy("cost_avg_6_rank", "id")
w15 = Window.partitionBy("city").orderBy("supplier_id_count_cost_avg_6", "id")
w16 = Window.partitionBy("city", "category")

enriched = (
    df_products
    .select(
        "*",
        F.ntile(4).over(w).alias("cost_bucket"),
        F.row_number().over(w2).alias("seq_no"),
        F.avg("cost").over(w3).alias("cost_avg_6"),
        F.avg("stock_qty").over(w4).alias("category_avg_stock_qty"),
    )
)

enriched_products = (
    enriched
    .select(
        "*",
        F.row_number().over(w_hist).alias("seq_no_2"),
        F.first("seq_no", ignorenulls=True).over(w6).alias("first_known_seq_no"),
        F.avg("cost").over(w7).alias("cost_avg_3"),
        F.rank().over(w8).alias("cost_avg_6_rank"),
    )
)

not_flagged = (
    enriched_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.store_id") == 2), "left_anti")
)

metrics = (
    not_flagged
    .select(
        "id",
        "category",
        "brand",
        "tags",
        "supplier_id",
        "seq_no",
        "cost_avg_6",
        "cost_avg_6_rank",
        F.first("cost_avg_3", ignorenulls=True).over(w9).alias("first_known_cost_avg_3"),
        F.row_number().over(w).alias("row_seq"),
    )
)

enriched_products_2 = (
    metrics
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.rating").between(2.4, 3.5))
    .select("e.*", F.col("d.rating"), F.col("d.country"), F.col("d.city"))
)

scored = (
    enriched_products_2
    .select(
        "id",
        "category",
        "first_known_cost_avg_3",
        "row_seq",
        "rating",
        "city",
        F.avg("row_seq").over(w10).alias("row_seq_avg_2"),
        F.sum("seq_no").over(w11).alias("running_seq_no"),
        F.lag("cost_avg_6", 1).over(w12).alias("prev_cost_avg_6"),
        F.count("cost_avg_6").over(w13).alias("supplier_id_count_cost_avg_6"),
        F.row_number().over(w14).alias("event_seq"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "category",
        "rating",
        "city",
        "running_seq_no",
        "event_seq",
        F.lead("rating").over(w15).alias("next_rating"),
        F.lag("rating").over(w15).alias("prev_rating"),
        F.lag("row_seq").over(w15).alias("prev_row_seq"),
        F.row_number().over(w15).alias("row_seq_2"),
        F.round((F.col("rating") / F.sum("rating").over(w16)), 4).alias("rating_share"),
    )
)

pivoted = (
    with_windows
    .groupBy("category")
    .pivot("city", ["New York", "Singapore"])
    .agg(F.avg("row_seq_2").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "category",
        F.col("New York_amount").alias("new_york_amount"),
        F.col("New York_n").alias("new_york_n"),
        F.col("Singapore_amount").alias("singapore_amount"),
        F.col("Singapore_n").alias("singapore_n"),
    )
)

result = (
    pivoted
    .select("category", "new_york_n", "singapore_amount", "singapore_n")
    .orderBy(F.col("singapore_n").asc_nulls_last(), F.col("singapore_amount").desc())
)
