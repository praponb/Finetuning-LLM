from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("category").orderBy("launch_date", "id")
w2 = Window.partitionBy("category", "brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("supplier_id").orderBy("next_stock_qty", "id").rowsBetween(-3, Window.currentRow)

with_windows = (
    df_products
    .withColumn("next_cost", F.lead("cost").over(w_running))
    .withColumn("prev_cost", F.lag("cost").over(w_running))
    .withColumn("next_stock_qty", F.lead("stock_qty").over(w_running))
    .withColumn("row_seq", F.row_number().over(w_running))
    .withColumn("last_known_price", F.last("price", ignorenulls=True).over(w2))
)

enriched = (
    with_windows
    .alias("s")
    .join(df_suppliers.alias("b"), F.col("s.supplier_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.product_name",
        "s.brand",
        "s.price",
        "s.launch_date",
        "s.supplier_id",
        "s.next_cost",
        "s.prev_cost",
        "s.next_stock_qty",
        "s.row_seq",
        "s.last_known_price",
        F.col("b.country"),
        F.col("b.supplier_name"),
        F.col("b.city"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "next_cost",
        "prev_cost",
        "row_seq",
        "last_known_price",
        "country",
        "city",
        F.count("next_cost").over(w_ordered).alias("last4_count_next_cost"),
        F.avg("next_cost").over(w_ordered).alias("last4_avg_next_cost"),
        F.sum("next_cost").over(w_ordered).alias("last4_sum_next_cost"),
        F.max("next_cost").over(w_ordered).alias("last4_max_next_cost"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.store_id") == 3), "left_anti")
)

result = (
    not_flagged
    .select("id", "city", "last4_sum_next_cost", "next_cost")
    .unpivot(
        ids=["id", "city"],
        values=["last4_sum_next_cost", "next_cost"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
