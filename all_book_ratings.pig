
/*** best 5 books per city

all tables

output city isbn authorname year and publisher average rating

step 1 group by location and isbn
 ***/
REGISTER '/home/hai/Tools/pig-0.17.0/lib/piggybank.jar';

-- loading the books csv and attaching names and types to each field
books  = LOAD '/user/pig/books/input/BX-Books.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(';', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (ISBN:chararray, book_title:chararray, book_author:chararray, year_of_publication:int ,publisher:chararray, Image_URL_S:chararray, Image_URL_M:chararray, Image_URL_L:chararray);
-- loading the rating csv and attaching names and types to each field
ratingraw  = LOAD '/user/pig/books/input/BX-Book-Ratings.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(';', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (user_id:int,ISBN:chararray, rating:int);

-- filtering rating and removing 0 rating because the data set 0 is an implied rating and there are allot of them.
 rating = FILTER ratingraw BY NOT(rating ==0);

--joinging rating and books by isbn
first_join = JOIN rating BY ISBN , books BY ISBN;

books_and_ratings = FOREACH first_join GENERATE 
 rating::ISBN AS ISBN,rating AS rating, book_title AS book_title , book_author AS book_author , year_of_publication AS year_of_publication, publisher AS publisher;
 
 
 


 

--grouping based on isbn
grp_by_ISBN = GROUP books_and_ratings BY ISBN;

--using generate to flatten groups and have an average rating for each group
 
flattened_ISBN = FOREACH grp_by_ISBN GENERATE
    FLATTEN(group) AS (ISBN),
    FLATTEN(books_and_ratings.book_title),
    FLATTEN(books_and_ratings.book_author),
    FLATTEN(books_and_ratings.year_of_publication),
    AVG(books_and_ratings.rating) AS AVGrating;


                        
                     
   

   
STORE flattened_ISBN INTO '/home/hai/outputdata/allbookratings';