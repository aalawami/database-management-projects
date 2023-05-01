SELECT COUNT(DISTINCT Categories.category)
FROM ((Categories
INNER JOIN Products ON Categories.item_id = Products.item_id)
INNER JOIN Bid ON Categories.item_id = Bid.item_id)
WHERE amount > 100;