from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first())

kept_keys = (
    df_suppliers
    .select("city")
    .subtract(
        df_suppliers
        .filter(F.col("rating") > 2.0)
        .select("city")
    )
)

reconciled = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
    )
)

scored = (
    cleaned
    .withColumn("country_sum_rating", F.sum("rating").over(w))
    .withColumn("rating_percent_rank", F.percent_rank().over(w2))
)

kept = (
    scored
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("ch.tags"), "bestseller")), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.stock_qty") >= 147), "left_anti")
)

result = (
    not_flagged
    .select("country", "rating_filled", "rating")
    .unpivot(
        ids=["country"],
        values=["rating_filled", "rating"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
