from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("city")
w5 = Window.partitionBy("country")
w6 = Window.partitionBy("country", "city").orderBy(F.col("rating_share").desc(), "id")
w7 = Window.partitionBy("country").orderBy("seq_no", "id")

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.length("supplier_name") >= 10))
    .select(
        "*",
        F.when(F.col("rating") >= 4.4, "High").when(F.col("rating") >= 1.5, "Medium").otherwise("Low").alias("rating_band"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.lead("rating").over(w).alias("next_rating"),
        F.row_number().over(w).alias("seq_no"),
        F.sum("rating").over(w2).alias("cumulative_rating"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.avg("next_rating").over(w3).alias("next_rating_avg_2"),
        F.avg("seq_no").over(w4).alias("city_avg_seq_no"),
        F.round((F.col("rating") / F.sum("rating").over(w5)), 4).alias("rating_share"),
    )
)

ranked = (
    metrics
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

latest = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "country",
        F.col("rating_share").alias("RATING_SHARE"),
        F.col("rating").alias("RATING"),
    )
    .unpivot(
        ids=["id", "country"],
        values=["RATING_SHARE", "RATING"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
