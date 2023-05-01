drop table if exists Products;
drop table if exists Bid;
drop table if exists Categories;
drop table if exists User;



CREATE TABLE Products (
item_id INTEGER NOT NULL,
user_id TEXT NOT NULL,
name TEXT NOT NULL,
number_of_bids INTEGER NOT NULL,
currently REAL NOT NULL,
first_bid REAL NOT NULL,
buy_price REAL,
started TEXT NOT NULL,
ends TEXT NOT NULL,
description TEXT,
PRIMARY KEY (item_id)
);

CREATE TABLE Bid (
item_id INTEGER NOT NULL,
user_id TEXT NOT NULL,
date_time TEXT NOT NULL,
amount DOUBLE NOT NULL,
PRIMARY KEY (item_id, user_id, date_time),
FOREIGN KEY (item_id) REFERENCES Products (item_id),
FOREIGN KEY (user_id) REFERENCES User (user_id)
);

CREATE TABLE Categories (
item_id INTEGER NOT NULL,
category TEXT,
PRIMARY KEY (item_id, category)
FOREIGN KEY (item_id) REFERENCES Products (item_id)
);

CREATE TABLE User (
user_id TEXT NOT NULL,
rating INTEGER NOT NULL,
location TEXT,
country TEXT,
PRIMARY KEY (user_id)
);
