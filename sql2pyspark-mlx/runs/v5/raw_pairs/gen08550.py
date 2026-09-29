from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(-3, -1)
w3 = Window.orderBy("units", "id")
w4 = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("sale_date", "id")
w6 = Window.orderBy(F.col("units").asc_nulls_last())
w7 = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc(), "id")

deduped = (
    df_store_sales
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.avg("revenue").over(w2).alias("revenue_avg_3_prior"),
        F.row_number().over(w3).alias("event_seq"),
        F.last("units", ignorenulls=True).over(w4).alias("last_known_units"),
        F.lag("revenue", 2).over(w5).alias("prev_revenue"),
        F.cume_dist().over(w6).alias("units_cume_dist"),
    )
)

deduped_store_sales = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched_store_sales = (
    deduped_store_sales
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.supplier_id").isNull())
    .select(
        "e.id",
        "e.product_id",
        "e.sale_date",
        "e.revenue",
        "e.revenue_avg_3_prior",
        "e.units_cume_dist",
        F.col("p.category"),
        F.col("p.cost"),
        F.col("p.brand"),
    )
)

result = (
    enriched_store_sales
    .select(
        "id",
        "revenue",
        "revenue_avg_3_prior",
        "units_cume_dist",
        "category",
        "cost",
        "brand",
    )
    .orderBy("category")
)
