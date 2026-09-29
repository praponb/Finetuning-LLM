from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("stock_qty", "id")
w2 = Window.partitionBy("brand")
w3 = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(-6, -1)
w4 = Window.orderBy(F.col("cost").desc(), "id")
w5 = Window.orderBy("cost_avg_6_prior", "id")
w6 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("supplier_id").orderBy("cost_avg_6_prior", "id")
w8 = Window.partitionBy("supplier_id", "city").orderBy("cost_bucket", "id")

not_flagged = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.product_id").isin([1, 3, 8])), "left_anti")
)

scored = (
    not_flagged
    .filter(F.size("tags") > 1)
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "tags",
        "supplier_id",
        F.lead("cost", 1, 0).over(w).alias("next_cost"),
        F.corr("stock_qty", "price").over(w2).alias("corr_stock_qty_price"),
        F.avg("cost").over(w3).alias("cost_avg_6_prior"),
    )
)

with_windows = (
    scored
    .withColumn("cost_bucket", F.ntile(4).over(w4))
    .withColumn("prev_cost", F.lag("cost", 1).over(w5))
    .withColumn("running_cost_avg_6_prior", F.sum("cost_avg_6_prior").over(w6))
    .withColumn("next_price", F.lead("price", 1).over(w7))
)

enriched = (
    with_windows
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id").eqNullSafe(F.col("d.id")), "left")
    .select("a.*", F.col("d.rating"), F.col("d.country"), F.col("d.city"))
)

not_flagged_products = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isin([6, 9])), "left_anti")
)

deduped = (
    not_flagged_products
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select("id", "supplier_id", "prev_cost", "next_price", "corr_stock_qty_price")
    .unpivot(
        ids=["id", "supplier_id"],
        values=["prev_cost", "next_price", "corr_stock_qty_price"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
