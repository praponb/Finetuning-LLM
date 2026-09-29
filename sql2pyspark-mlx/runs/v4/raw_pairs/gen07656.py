from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("store_sale_units", "id")
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("city").orderBy(F.col("store_sale_units").desc(), "id")
w_hist = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

not_flagged = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.product_id") == 1), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units").alias("store_sale_units"))
)

enriched = (
    latest_match
    .select(
        "*",
        F.lag("store_sale_units", 1).over(w2).alias("prev_store_sale_units"),
        F.sum("store_sale_units").over(w3).alias("cumulative_store_sale_units"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.ntile(4).over(w_ordered).alias("store_sale_units_bucket"),
        F.sum("cumulative_store_sale_units").over(w_hist).alias("running_cumulative_store_sale_units"),
    )
)

result = (
    scored
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        "cumulative_store_sale_units",
        "store_sale_units_bucket",
        F.when(F.col("store_name").rlike("^(Riverside|Old Town)$"), "CORE").when(F.col("store_name").rlike("^(Siam Flagship|Downtown Express)$"), "GROUP_A").otherwise("EXTENDED").alias("store_name_zone"),
    )
    .orderBy(F.col("store_name_zone").asc_nulls_last())
)
