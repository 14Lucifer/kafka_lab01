
CREATE TABLE products (
            product_id SERIAL PRIMARY KEY,
            product VARCHAR(255),
            brand VARCHAR(255),
            price FLOAT(20),
            quantity VARCHAR(255),
            category VARCHAR(255),
            sub_category VARCHAR(255)
        );

insert into products (product,brand,price,quantity,category,sub_category)
values ('Onion (Loose)','Fresho',69.75,'2 kg','Fruits & Vegetables','Potato, Onion & Tomato');
insert into products (product,brand,price,quantity,category,sub_category)
values ('Eggs - Regular','Fresho',100.00,'1 pack','Eggs, Meat & Fish','Farm Eggs');
insert into products (product,brand,price,quantity,category,sub_category)
values ('Disinfectant Toilet Cleaner Liquid - Original','Harpic',390.00,'1 L','Cleaning & Household','Toilet Cleaners');

update products set 50.00 where product='Onion (Loose)';
update products set 100.00 where product='Eggs - Regular';
update products set 80.00 where product='Disinfectant Toilet Cleaner Liquid - Original';


delete from products where product='Onion (Loose)';
delete from products where product='Eggs - Regular';
delete from products where product='Disinfectant Toilet Cleaner Liquid - Original';

select * from products;
select * from log_table;