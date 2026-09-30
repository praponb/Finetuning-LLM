from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("revenue").desc(), "id")
w2 = Window.orderBy("units", "id")
w_part_2 = Window.orderBy("sale_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("sale_date_plus_months", "id")
w5 = Window.orderBy(F.col("last_known_units").desc(), "id")
w6 = Window.orderBy(F.col("units_bucket").desc(), "id")
w7 = Window.orderBy("units_bucket", "id")
w8 = Window.orderBy(F.col("units").desc(), "id")

valid = (
    df_store_sales
    .filter(F.col("revenue").between(2400, 2500))
    .select(
        "id",
        "units",
        "revenue",
        F.add_months("sale_date", 1).alias("sale_date_plus_months"),
    )
)

enriched = (
    valid
    .filter(F.col("units").between(14, 36))
    .select(
        "*",
        F.row_number().over(w_part).alias("event_seq"),
        F.ntile(4).over(w2).alias("units_bucket"),
        F.last("units", ignorenulls=True).over(w_part_2).alias("last_known_units"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.lead("revenue").over(w_grp).alias("next_revenue"),
        F.lag("last_known_units").over(w_grp).alias("prev_last_known_units"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.ntile(4).over(w5).alias("last_known_units_bucket"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "revenue",
        "sale_date_plus_months",
        "event_seq",
        "units_bucket",
        "last_known_units",
        "row_seq",
        F.row_number().over(w6).alias("event_seq_2"),
        F.lag("last_known_units_bucket", 1).over(w7).alias("prev_last_known_units_bucket"),
        F.row_number().over(w8).alias("event_seq_3"),
    )
)

result = (
    metrics
    .select(
        "id",
        "revenue",
        "sale_date_plus_months",
        "event_seq",
        "units_bucket",
        "last_known_units",
        "row_seq",
        "event_seq_2",
        "prev_last_known_units_bucket",
    )
    .orderBy("sale_date_plus_months")
)
