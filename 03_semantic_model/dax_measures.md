```dax
Total SKUs Monitored = COUNTROWS('replenishment_recommendations')
```
```dax
Items To Reorder =
CALCULATE(COUNTROWS('replenishment_recommendations'),
          'replenishment_recommendations'[needs_reorder] = TRUE())
```
```dax
Stockout SKUs =
CALCULATE(COUNTROWS('replenishment_recommendations'),
          'replenishment_recommendations'[status] = "STOCKOUT")
```
```dax
Overstock SKUs =
CALCULATE(COUNTROWS('replenishment_recommendations'),
          'replenishment_recommendations'[status] = "OVERSTOCK")
```
```dax
Est Order Cost = SUM('replenishment_recommendations'[est_order_cost])
```
```dax
Total Stockout Risk = SUM('replenishment_recommendations'[risk_score])
```
```dax
Avg Days of Cover = AVERAGE('replenishment_recommendations'[days_of_cover])
```
```dax
Cash Tied Up =
SUMX(FILTER('replenishment_recommendations',
         'replenishment_recommendations'[status] = "OVERSTOCK"),
     'replenishment_recommendations'[on_hand_qty] *
     'replenishment_recommendations'[unit_cost])
```
```dax
In-Stock Service Level % =
DIVIDE(
    [Total SKUs Monitored] - [Stockout SKUs],
    [Total SKUs Monitored])
```

Perishable Overstock SKUs =
CALCULATE(COUNTROWS('replenishment_recommendations'),
          'replenishment_recommendations'[status] = "OVERSTOCK",
          'replenishment_recommendations'[is_perishable] = TRUE())
