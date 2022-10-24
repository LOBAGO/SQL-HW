Select CompanyName,round(dc*100.0/c,2) AS p
from
(select ShipVia,Count(*) AS c from 'order' Group BY ShipVia) As S
Inner Join
(select ShipVia, Count(*) AS dc from 'order' where ShippedDate > RequiredDate Group By ShipVia) AS dc On s.ShipVia = dc.ShipVia 
Inner Join Shipper on s.ShipVia = Shipper.id
Order by p DESC
;

