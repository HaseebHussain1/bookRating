
/*** best 5 books per city

all tables

output city isbn authorname year and publisher average rating

step 1 group by location and isbn
 ***/
REGISTER '/home/hai/Tools/pig-0.17.0/lib/piggybank.jar';
user_temp  = LOAD '/user/pig/books/input/BX-Users3.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(';', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (user_id:int, location: chararray, age:int);

books  = LOAD '/user/pig/books/input/BX-Books.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(';', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (ISBN:chararray, book_title:chararray, book_author:chararray, year_of_publication:int ,publisher:chararray, Image_URL_S:chararray, Image_URL_M:chararray, Image_URL_L:chararray);

ratingraw  = LOAD '/user/pig/books/input/BX-Book-Ratings.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(';', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (user_id:int,ISBN:chararray, rating:int);

 rating = FILTER ratingraw BY NOT(rating ==0);

user_tmp2 = FOREACH user_temp GENERATE user_id, flatten(STRSPLIT (location,',',3)) AS (county:chararray, city:chararray, country:chararray),age;

user = FOREACH user_tmp2 GENERATE user_id AS user_id, county AS county:chararray,city AS city:chararray, country AS country:chararray ,age;



nullvaluesremoved2 = FILTER user BY NOT((city MATCHES '\\W') OR (country MATCHES '\\W') OR (age is null) OR (age<40));
finalUsers2= FOREACH nullvaluesremoved2 GENERATE user_id AS user_id, county AS county,city AS city, country AS country ,age AS age;




first_join2 = JOIN finalUsers2 BY user_id , rating BY user_id;

user_and_ratings2 = FOREACH first_join2 GENERATE 
finalUsers2::user_id AS user_id,finalUsers2::city AS city,finalUsers2::country AS country,finalUsers2::age AS age, rating::ISBN AS ISBN,rating::rating AS rating;
describe user_and_ratings2;





second_join2 = JOIN user_and_ratings2 BY ISBN , books BY ISBN;

finaldata2 = FOREACH second_join2 GENERATE 
user_and_ratings2::user_id AS user_id,user_and_ratings2::city AS city,user_and_ratings2::country AS country,user_and_ratings2::age AS age, user_and_ratings2::ISBN AS ISBN,user_and_ratings2::rating AS rating, books::book_title AS book_title , books::book_author AS book_author , books::year_of_publication AS year_of_publication, books::publisher AS publisher;


grp_by_author = GROUP finaldata2 BY (author);
 flattened_by_author = FOREACH grp_by_author GENERATE
    FLATTEN(group) AS (book_author, ISBN),
    FLATTEN(finaldata2.book_title) ,
    FLATTEN(finaldata2.year_of_publication),
    FLATTEN(finaldata2.country),
    AVG(finaldata2.rating) AS AVGrating;

STORE flattened_by_author INTO '/home/hai/outputdata/authororig';