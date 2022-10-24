with person AS
(
Select Product.Id, Product.ProductName as name
From Product
Inner Join OrderDetail on Product.id = OrderDetail.ProductId
Inner Join 'Order' on 'Order'.Id = OrderDetail.OrderId
Inner Join Customer on CustomerId = Customer.Id
Where DATE(OrderDate) = '2014-12-25' and CompanyName = 'Queen Cozinha'
Group By Product.id
),
c AS 
(
Select row_number() OVER (Order By p.id ) as seqnum, person.name as name
From person
),
flattened AS (
Select seqnum, name as name
From c
Where seqnum = 1
Union all
Select c.seqnum, f.name || ', ' || c.name
From c join
 flattened f
 on c.seqnum = f.seqnum + 1
)
select name from flattened
order by seqnum desc limit 1;
