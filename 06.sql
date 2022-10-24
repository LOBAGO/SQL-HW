Select Id,OrderDate,POD,Round(julianday(OrderDate)-julianday(POD),2)
From
(
Select Id,OrderDate,LAG(OrderDate,1,OrderDate) OVER (Order By OrderDate) AS POD
From 'Order'
Where CustomerId = 'BLONP'
Order By OrderDate
Limit 10
)
;

