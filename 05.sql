Select PN,CompanyName,ContactName
From
(Select PN,min(OrderDate),CompanyName,ContactName From(Select Id AS pid, ProductName AS PN From Product Where Discontinued = 1)
Inner Join OrderDetail on ProductId = pid
Inner Join 'Order' on 'Order'.Id = OrderDetail.OrderId
Inner Join Customer on CustomerID = Customer.Id
Group By pid
)
Order By PN ASC
;

