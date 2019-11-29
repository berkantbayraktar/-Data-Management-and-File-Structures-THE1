package ceng.ceng351.bookdb;

import com.mysql.cj.xdevapi.SqlStatement;

import java.sql.*;
import java.util.ArrayList;

public class BOOKDB implements IBOOKDB {
    /**
     * Place your initialization code inside if required.
     *
     * <p>
     * This function will be called before all other operations. If your
     * implementation need initialization , necessary operations should be done
     * inside this function. For example, you can set your connection to the
     * database server inside this function.
     */
    private static String user = "2098796";
    private static String password = "2616558b";
    private static String host = "144.122.71.65";
    private static String database = "db2098796";
    private static int port = 8084; // port
    private static Connection connection = null;
    private Statement statement = null;

    @Override
    public void initialize() {

        String url = "jdbc:mysql://" + host + ":" + port + "/" + database;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        dropTables();

    }

    /**
     * Should create the necessary tables when called.
     *
     * @return the number of tables that are created successfully.
     */
    @Override
    public int createTables() {
        String sql;


        sql = "CREATE TABLE IF NOT EXISTS author " +
                "(author_id INTEGER not NULL, " +
                " author_name VARCHAR(60), " +
                " PRIMARY KEY (author_id))";

        try {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }



        sql = "CREATE TABLE IF NOT EXISTS publisher "+
                "(publisher_id INTEGER not NULL, " +
                " publisher_name VARCHAR(50), " +
                " PRIMARY KEY (publisher_id))";

        try {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }




        sql = "CREATE TABLE IF NOT EXISTS book " +
                "(isbn CHAR(13), " +
                "book_name VARCHAR(120), "+
                "publisher_id INTEGER, "+
                "first_publish_year CHAR(4), "+
                "page_count INTEGER, "+
                "category VARCHAR(25), "+
                "rating FLOAT, "+
                "PRIMARY KEY (isbn), "+
                "FOREIGN KEY (publisher_id) REFERENCES publisher(publisher_id))";

        try {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }



        sql = "CREATE TABLE IF NOT EXISTS author_of" +
                "(isbn CHAR(13), "+
                "author_id INTEGER, " +
                "PRIMARY KEY (isbn,author_id), "+
                "FOREIGN KEY (isbn) REFERENCES book(isbn), " +
                "FOREIGN KEY (author_id) REFERENCES  author(author_id))";

        try {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }



        sql = "CREATE TABLE IF NOT EXISTS phw1" +
                "(isbn CHAR(13), " +
                "book_name VARCHAR(120), " +
                "rating FLOAT, " +
                "PRIMARY KEY (isbn))";

        try {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }



        System.out.println("completed.");
        return 5;
    }

    /**
     * Should drop the tables if exists when called.
     *
     * @return the number of tables are dropped successfully.
     */
    @Override
    public int dropTables() {

        String sql;

        sql = "DROP TABLE IF EXISTS phw1";

        try {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        sql = "DROP TABLE IF EXISTS author_of";

        try {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        sql = "DROP TABLE IF EXISTS book";

        try {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        sql = "DROP TABLE IF EXISTS publisher";

        try {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        sql = "DROP TABLE IF EXISTS author";
        try {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 5;
    }

    /**
     * Should insert an array of Author into the database.
     *
     * @param authors
     * @return Number of rows inserted successfully.
     */
    @Override
    public int insertAuthor(Author[] authors) {
        String sql;
        int count = 0;

        for(Author author: authors){
            sql = "INSERT INTO author (author_id, author_name) " +
                    "VALUES (?,?)";

            try {
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setInt(1,author.getAuthor_id());
                preparedStatement.setString(2,author.getAuthor_name());
                preparedStatement.executeUpdate();
                count++;
                System.out.println("my statement : " + sql);

            } catch (SQLException e) {
                e.printStackTrace();
            }


        }

        System.out.println("Count : " + count);
        return count;
    }

    /**
     * Should insert an array of Book into the database.
     *
     * @param books
     * @return Number of rows inserted successfully.
     */
    @Override
    public int insertBook(Book[] books) {

        String sql;
        int count = 0;

        for(Book book : books){
            sql = "INSERT INTO book (isbn,book_name,publisher_id,first_publish_year,page_count,category,rating) " +
                    "VALUES (?,?,?,?,?,?,?)";

            try {
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setString(1,book.getIsbn());
                preparedStatement.setString(2,book.getBook_name());
                preparedStatement.setInt(3,book.getPublisher_id());
                preparedStatement.setString(4,book.getFirst_publish_year());
                preparedStatement.setInt(5,book.getPage_count());
                preparedStatement.setString(6,book.getCategory());
                preparedStatement.setDouble(7,book.getRating());
                preparedStatement.executeUpdate();
                count++;
                System.out.println("my statement : " + sql);

            } catch (SQLException e) {
                e.printStackTrace();
            }


        }

        System.out.println("Count : " + count);
        return count;
    }

    /**
     * Should insert an array of Publisher into the database.
     *
     * @param publishers
     * @return Number of rows inserted successfully.
     */
    @Override
    public int insertPublisher(Publisher[] publishers) {

        String sql;
        int count = 0;

        for(Publisher publisher: publishers){
            sql = "INSERT INTO publisher (publisher_id, publisher_name) " +
                    "VALUES (?,?)";

            try {
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setInt(1,publisher.getPublisher_id());
                preparedStatement.setString(2,publisher.getPublisher_name());
                preparedStatement.executeUpdate();
                count++;
                System.out.println("my statement : " + sql);

            } catch (SQLException e) {
                e.printStackTrace();
            }


        }

        System.out.println("Count : " + count);
        return count;

    }

    /**
     * Should insert an array of Author_of into the database.
     *
     * @param author_ofs
     * @return Number of rows inserted successfully.
     */
    @Override
    public int insertAuthor_of(Author_of[] author_ofs) {

        String sql;
        int count = 0;

        for(Author_of author_of: author_ofs){
            sql = "INSERT INTO author_of (isbn, author_id) " +
                    "VALUES (?,?)";

            try {
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setString(1,author_of.getIsbn());
                preparedStatement.setInt(2,author_of.getAuthor_id());
                preparedStatement.executeUpdate();
                count++;
                System.out.println("my statement : " + sql);

            } catch (SQLException e) {
                e.printStackTrace();
            }


        }

        System.out.println("Count : " + count);
        return count;
    }

    /**
     * Should return isbn, first_publish_year, page_count and publisher_name of
     * the books which have the maximum number of pages.
     *
     * @return QueryResult.ResultQ1[]
     */
    @Override
    public QueryResult.ResultQ1[] functionQ1() {

        String query;
        ResultSet resultset = null;
        ArrayList<QueryResult.ResultQ1> answer = new ArrayList<>();

        query = "SELECT b.isbn, b.first_publish_year, b.page_count, p.publisher_name FROM db2098796.book b , db2098796.publisher p \n" +
                "WHERE b.publisher_id = p.publisher_id AND \n" +
                "\t b.page_count >= ALL(SELECT b2.page_count FROM db2098796.book b2) \n" +
                "order by b.isbn;";

        try{
            resultset = statement.executeQuery(query);
        }
        catch (SQLException e ){
            e.printStackTrace();
        }


        try{
            while(resultset.next()){
                String isbn = resultset.getString("isbn");
                String first_publish_year = resultset.getString("first_publish_year");
                int page_count = resultset.getInt("page_count");
                String publisher_name = resultset.getString("publisher_name");

                QueryResult.ResultQ1 item = new QueryResult.ResultQ1(isbn,first_publish_year,page_count,publisher_name);
                answer.add(item);
            }
        }
        catch (SQLException e ){
            e.printStackTrace();
        }


        QueryResult.ResultQ1 [] res = answer.toArray(new QueryResult.ResultQ1[answer.size()]);
        return res;
    }

    /**
     * For the publishers who have published books that were co-authored by both
     * of the given authors(author1 and author2); list publisher_id(s) and average
     * page_count(s)  of all the books these publishers have published.
     *
     * @param author_id1
     * @param author_id2
     * @return QueryResult.ResultQ2[]
     */
    @Override
    public QueryResult.ResultQ2[] functionQ2(int author_id1, int author_id2) {
        return new QueryResult.ResultQ2[0];
    }

    /**
     * List book_name, category and first_publish_year of the earliest
     * published book(s) of the author(s) whose author_name is given.
     *
     * @param author_name
     * @return QueryResult.ResultQ3[]
     */
    @Override
    public QueryResult.ResultQ3[] functionQ3(String author_name) {

        String query;
        ResultSet resultset = null;
        ArrayList<QueryResult.ResultQ3> answer = new ArrayList<>();

        query = "SELECT b.book_name, b.category, b.first_publish_year FROM book b, author a , author_of o \n" +
                "WHERE o.author_id = a.author_id AND a.author_name = (?) AND b.isbn = o.isbn AND \n" +
                "\tb.first_publish_year <= ALL (SELECT b2.first_publish_year FROM book b2, author a2, author_of o2\n" +
                "                                WHERE o2.author_id = a2.author_id AND a2.author_name = (?) AND b2.isbn = o2.isbn)\n" +
                "ORDER BY b.book_name, b.category, b.first_publish_year;";

        try{
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1,author_name);
            preparedStatement.setString(2,author_name);
            resultset = preparedStatement.executeQuery();
        }

        catch (SQLException e) {
            e.printStackTrace();
        }

        try{
            while(resultset.next()){
                String book_name = resultset.getString("book_name");
                String category = resultset.getString("category");
                String first_publish_year = resultset.getString("first_publish_year");

                QueryResult.ResultQ3 item = new QueryResult.ResultQ3(book_name,category,first_publish_year);
                answer.add(item);
            }
        }
        catch (SQLException e ){
            e.printStackTrace();
        }

        QueryResult.ResultQ3 [] res = answer.toArray(new QueryResult.ResultQ3[answer.size()]);
        return res;
    }

    /**
     * For publishers whose name contains at least 3 words
     * (i.e., "Koc Universitesi Yayinlari"), have published at least 3 books
     * and average rating of their books are greater than(>) 3;
     * list their publisher_id(s) and distinct category(ies) they have published.
     * PS: You may assume that each word in publisher_name is seperated by a space.
     *
     * @return QueryResult.ResultQ4[]
     */
    @Override
    public QueryResult.ResultQ4[] functionQ4() {

        String query;
        ResultSet resultset = null;
        ArrayList<QueryResult.ResultQ4> answer = new ArrayList<>();

        query = "SELECT DISTINCt b2.publisher_id, b2.category FROM db2098796.book b2\n" +
                "WHERE b2.publisher_id IN \n" +
                "(SELECT b.publisher_id FROM db2098796.book b , db2098796.publisher p\n" +
                "WHERE p.publisher_id = b.publisher_id AND p.publisher_name LIKE '% % %'\n" +
                "GROUP BY b.publisher_id \n" +
                "HAVING COUNT(*) > 2 AND AVG(b.rating) >3)\n" +
                "ORDER BY b2.publisher_id , b2.category;";

        try{
            resultset = statement.executeQuery(query);
        }
        catch (SQLException e ){
            e.printStackTrace();
        }


        try{
            while(resultset.next()){
                int publisher_id = resultset.getInt("publisher_id");
                String category = resultset.getString("category");

                QueryResult.ResultQ4 item = new QueryResult.ResultQ4(publisher_id,category);
                answer.add(item);
            }
        }
        catch (SQLException e ){
            e.printStackTrace();
        }


        QueryResult.ResultQ4 [] res = answer.toArray(new QueryResult.ResultQ4[answer.size()]);
        return res;

    }

    /**
     * List author_id and author_name of the authors who have worked with
     * all the publishers that the given author_id has worked.
     *
     * @param author_id
     * @return QueryResult.ResultQ5[]
     */
    @Override
    public QueryResult.ResultQ5[] functionQ5(int author_id) {

        String query;
        ResultSet resultset = null;
        ArrayList<QueryResult.ResultQ5> answer = new ArrayList<>();

        query = "SELECT a.author_id, a.author_name FROM db2098796.author a WHERE NOT EXISTS (\n" +
                "\tSELECT b2.publisher_id FROM db2098796.book b2 , db2098796.author_of o2\n" +
                "    WHERE b2.isbn = o2.isbn AND o2.author_id = (?) AND b2.publisher_id NOT IN\n" +
                "(SELECT b.publisher_id FROM db2098796.book b , db2098796.author_of o WHERE a.author_id = o.author_id AND b.isbn = o.isbn))\n" +
                "ORDER BY a.author_id;";

        try{
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setInt(1,author_id);
            resultset = preparedStatement.executeQuery();
        }

        catch (SQLException e) {
            e.printStackTrace();
        }

        try{
            while(resultset.next()){

                int author_ID = resultset.getInt("author_id");
                String author_name = resultset.getString("author_name");

                QueryResult.ResultQ5 item = new QueryResult.ResultQ5(author_ID,author_name);
                answer.add(item);
            }
        }
        catch (SQLException e ){
            e.printStackTrace();
        }

        QueryResult.ResultQ5 [] res = answer.toArray(new QueryResult.ResultQ5[answer.size()]);
        return res;

    }

    /**
     * List author_name(s) and isbn(s) of the book(s) written by "Selective" authors.
     * "Selective" authors are those who have worked with publishers that have
     * published their books only.(i.e., they haven't published books of
     * different authors)
     *
     * @return QueryResult.ResultQ6[]
     */
    @Override
    public QueryResult.ResultQ6[] functionQ6() {
        return new QueryResult.ResultQ6[0];
    }

    /**
     * List publisher_id and publisher_name of the publishers who have published
     * at least 2 books in  'Roman' category and average rating of their books
     * are more than (>) the given value.
     *
     * @param rating
     * @return QueryResult.ResultQ7[]
     */
    @Override
    public QueryResult.ResultQ7[] functionQ7(double rating) {
        return new QueryResult.ResultQ7[0];
    }

    /**
     * Some of the books  in the store have been published more than once:
     * although they have same names(book\_name), they are published with different
     * isbns. For each  multiple copy of these books, find the book_name(s) with the
     * lowest rating for each book_name  and store their isbn, book_name and
     * rating into phw1 table using a single BULK insertion query.
     * If there exists more than 1 with the lowest rating, then store them all.
     * After the bulk insertion operation, list isbn, book_name and rating of
     * all rows in phw1 table.
     *
     * @return QueryResult.ResultQ8[]
     */
    @Override
    public QueryResult.ResultQ8[] functionQ8() {
        return new QueryResult.ResultQ8[0];
    }

    /**
     * For the books that contain the given keyword anywhere in their names,
     * increase their ratings by one.
     * Please note that, the maximum rating cannot be more than 5,
     * therefore if the previous rating is greater than 4, do not update the
     * rating of that book.
     *
     * @param keyword
     * @return sum of the ratings of all books
     */
    @Override
    public double functionQ9(String keyword) {
        return 0;
    }

    /**
     * Delete publishers in publisher table who haven't published a book yet.
     *
     * @return count of all rows of the publisher table after delete operation.
     */
    @Override
    public int function10() {
        return 0;
    }
}
