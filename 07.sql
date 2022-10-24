With Expend As
(
Select IFNULL(c.CompanyName,'MISSING_NAME') AS CompanyName,o.CustomerId,Round(SUM(od.Quantity*od.UnitPrice),2) AS TC
From 'Order' AS o
Inner Join OrderDetail od on od.OrderId = o.Id
Left Join Customer c on c.Id = o.CustomerId
Group By o.CustomerId
),
Q AS
(
Select*,NTILE(4) OVER (Order By TC) AS EQ
From Expend
)
Select CompanyName,CustomerId,TC
From Q
Where EQ=1
Order By TC
;

