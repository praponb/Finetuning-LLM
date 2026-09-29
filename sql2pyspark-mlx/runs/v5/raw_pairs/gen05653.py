from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sqft", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue").asc_nulls_first())

cleaned = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 5, 8, 10]))
    .withColumn("adjusted_units", F.round((F.col("units") * 0.85), 2))
)

enriched = (
    cleaned
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.sqft"), F.col("b.opened_date"))
)

with_windows = (
    enriched
    .select(
        "*",
        F.lag("sqft").over(w).alias("prev_sqft"),
        F.lead("units").over(w).alias("next_units"),
        F.lag("units").over(w).alias("prev_units"),
        F.row_number().over(w).alias("seq_no"),
        F.dense_rank().over(w2).alias("revenue_dense_rank"),
    )
)

base = (
    with_windows
    .filter(F.col("adjusted_units") < 23)
    .select(
        "*",
        F.when(F.col("revenue") >= 3500, "High").when(F.col("revenue") >= 600, "Medium").otherwise("Low").alias("revenue_band"),
    )
)

result = (
    base
    .select("id", "store_id", "adjusted_units", "revenue")
    .unpivot(
        ids=["id", "store_id"],
        values=["adjusted_units", "revenue"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
)
