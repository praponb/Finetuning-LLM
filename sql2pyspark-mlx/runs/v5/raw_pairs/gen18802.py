from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "category").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("category").orderBy("cost", "id")
w3 = Window.partitionBy("brand").orderBy("price", "id")
w4 = Window.partitionBy("category", "supplier_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("brand").orderBy(F.col("row_seq").desc(), "id")

scored = (
    df_products
    .withColumn("cost_bucket", F.ntile(4).over(w))
    .withColumn("cost_bucket_2", F.ntile(5).over(w2))
)

not_flagged = (
    scored
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") >= F.col("s.launch_date")), "left_semi")
)

with_windows = (
    not_flagged
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "tags",
        F.lead("cost_bucket").over(w3).alias("next_cost_bucket"),
        F.lag("cost_bucket").over(w3).alias("prev_cost_bucket"),
        F.lag("price").over(w3).alias("prev_price"),
        F.lead("price").over(w3).alias("next_price"),
        F.row_number().over(w3).alias("row_seq"),
        F.first("brand", ignorenulls=True).over(w4).alias("first_known_brand"),
    )
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select("id", "brand", "next_price", "prev_price", "cost")
    .unpivot(
        ids=["id", "brand"],
        values=["next_price", "prev_price", "cost"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
