package ceng.ceng351.bookdb;
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



        //System.out.println("completed.");
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
                //System.out.println("my statement : " + sql);

            } catch (SQLException e) {
                e.printStackTrace();
            }


        }

        //System.out.println("Count : " + count);
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
                //System.out.println("my statement : " + sql);

            } catch (SQLException e) {
                e.printStackTrace();
            }


        }

        //System.out.println("Count : " + count);
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
                //System.out.println("my statement : " + sql);

            } catch (SQLException e) {
                e.printStackTrace();
            }


        }

        //System.out.println("Count : " + count);
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
                //System.out.println("my statement : " + sql);

            } catch (SQLException e) {
                e.printStackTrace();
            }


        }

        //System.out.println("Count : " + count);
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

        query = "SELECT b.isbn, b.first_publish_year, b.page_count, p.publisher_name FROM book b , publisher p \n" +
                "WHERE b.publisher_id = p.publisher_id AND \n" +
                "\t b.page_count >= ALL(SELECT b2.page_count FROM book b2) \n" +
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

        String query;
        ResultSet resultset = null;
        ArrayList<QueryResult.ResultQ2> answer = new ArrayList<>();

        query = "SELECT b.publisher_id, AVG(b.page_count) as average_page_count FROM db2098796.book b\n" +
                "GROUP BY b.publisher_id\n" +
                "HAVING b.publisher_id IN ( \n" +
                "\tSELECT b2.publisher_id\n" +
                "\tFROM db2098796.book b2\n" +
                "    WHERE b2.isbn IN (\n" +
                "\t\tSELECT o.isbn\n" +
                "        FROM db2098796.author_of o\n" +
                "        WHERE o.author_id = (?) AND o.isbn IN \n" +
                "\t\t\t(SELECT o2.isbn\n" +
                "\t\t\tFROM db2098796.author_of o2\n" +
                "\t\t\tWHERE o2.author_id = (?))))\n" +
                "order by b.publisher_id;";

        try{
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setInt(1,author_id1);
            preparedStatement.setInt(2,author_id2);
            resultset = preparedStatement.executeQuery();
        }

        catch (SQLException e) {
            e.printStackTrace();
        }

        try{
            while(resultset.next()){

                int publisher_id = resultset.getInt("publisher_id");
                double average_page_count = resultset.getDouble("average_page_count");
                QueryResult.ResultQ2 item = new QueryResult.ResultQ2(publisher_id,average_page_count);
                answer.add(item);
            }
        }
        catch (SQLException e ){
            e.printStackTrace();
        }

        QueryResult.ResultQ2 [] res = answer.toArray(new QueryResult.ResultQ2[answer.size()]);
        return res;
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

        query = "SELECT DISTINCT b2.publisher_id, b2.category FROM book b2\n" +
                "WHERE b2.publisher_id IN \n" +
                "(SELECT b.publisher_id FROM book b , publisher p\n" +
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

        query = "SELECT a.author_id, a.author_name FROM author a WHERE NOT EXISTS (\n" +
                "\tSELECT b2.publisher_id FROM book b2 , author_of o2\n" +
                "    WHERE b2.isbn = o2.isbn AND o2.author_id = (?) AND b2.publisher_id NOT IN\n" +
                "(SELECT b.publisher_id FROM book b , author_of o WHERE a.author_id = o.author_id AND b.isbn = o.isbn))\n" +
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

        String query;
        ResultSet resultset = null;
        ArrayList<QueryResult.ResultQ6> answer = new ArrayList<>();

        query = "SELECT aut.author_id,bo.isbn FROM db2098796.author aut, db2098796.book bo , db2098796.author_of autof\n" +
                "WHERE aut.author_id = autof.author_id AND bo.isbn = autof.isbn AND NOT EXISTS (\n" +
                "SELECT bo2.publisher_id FROM db2098796.author aut2, db2098796.book bo2 , db2098796.author_of autof2\n" +
                "WHERE aut.author_id = aut2.author_id AND aut2.author_id = autof2.author_id AND bo2.isbn = autof2.isbn AND bo2.publisher_id NOT IN\n" +
                "\n" +
                "( SELECT DISTINCT b.publisher_id FROM db2098796.book b, db2098796.book b2, db2098796.author_of o , db2098796.author_of o2\n" +
                " WHERE o.isbn = b.isbn AND o2.isbn = b2.isbn AND b.publisher_id = b2.publisher_id AND o2.author_id = o.author_id \n" +
                " AND b.publisher_id  NOT IN     \n" +
                " (SELECT DISTINCT b3.publisher_id  FROM db2098796.book b3, db2098796.book b4, db2098796.author_of o3 , db2098796.author_of o4  \n" +
                " WHERE o3.isbn = b3.isbn AND o4.isbn = b4.isbn AND b3.publisher_id = b4.publisher_id AND o4.author_id <> o3.author_id)) )\n" +
                "ORDER BY aut.author_id, bo.isbn;\n";

        try{
            resultset = statement.executeQuery(query);
        }
        catch (SQLException e ){
            e.printStackTrace();
        }


        try{
            while(resultset.next()){
                int author_id = resultset.getInt("author_id");
                String isbn = resultset.getString("isbn");
                QueryResult.ResultQ6 item = new QueryResult.ResultQ6(author_id,isbn);
                answer.add(item);
            }
        }
        catch (SQLException e ){
            e.printStackTrace();
        }


        QueryResult.ResultQ6 [] res = answer.toArray(new QueryResult.ResultQ6[answer.size()]);
        return res;


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

        String query;
        ResultSet resultset = null;
        ArrayList<QueryResult.ResultQ7> answer = new ArrayList<>();

        query = "SELECT p.publisher_id, p.publisher_name FROM book b, publisher p\n" +
                "WHERE p.publisher_id = b.publisher_id AND b.category = 'Roman'\n" +
                "GROUP BY p.publisher_id\n" +
                "HAVING COUNT(*) >1 AND AVG(b.rating) > (?) \n" +
                "ORDER BY p.publisher_id;";

        try{
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setDouble(1,rating);
            resultset = preparedStatement.executeQuery();
        }

        catch (SQLException e) {
            e.printStackTrace();
        }

        try{
            while(resultset.next()){

                int publisher_id = resultset.getInt("publisher_id");
                String publisher_name = resultset.getString("publisher_name");

                QueryResult.ResultQ7 item = new QueryResult.ResultQ7(publisher_id,publisher_name);
                answer.add(item);
            }
        }
        catch (SQLException e ){
            e.printStackTrace();
        }

        QueryResult.ResultQ7 [] res = answer.toArray(new QueryResult.ResultQ7[answer.size()]);
        return res;

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

        String sql;
        String query;
        ResultSet resultset = null;
        ArrayList<QueryResult.ResultQ8> answer = new ArrayList<>();

        sql = "INSERT INTO db2098796.phw1\n" +
                "SELECT DISTINCT b.isbn, b.book_name, b.rating\n" +
                "FROM db2098796.book b, db2098796.book b2\n" +
                "WHERE b.isbn <> b2.isbn AND b.book_name = b2.book_name AND b.isbn NOT IN \n" +
                "\t(SELECT DISTINCT  b3.isbn\n" +
                "\tFROM db2098796.book b3, db2098796.book b4\n" +
                "\tWHERE b3.isbn <> b4.isbn AND b3.book_name = b4.book_name AND b3.rating > b4.rating\n" +
                "\t)";

        try{
            statement.executeUpdate(sql);
        }
        catch (SQLException e ){
            e.printStackTrace();
        }

        query = "SELECT p.isbn, p.book_name, p.rating\n" +
                "FROM db2098796.phw1 p\n" +
                "ORDER BY p.isbn;";

        try{
            resultset = statement.executeQuery(query);
        }
        catch (SQLException e ){
            e.printStackTrace();
        }

        try{
            while(resultset.next()){
                String isbn = resultset.getString("isbn");
                String book_name = resultset.getString("book_name");
                double rating = resultset.getDouble("rating");

                ;

                QueryResult.ResultQ8 item = new QueryResult.ResultQ8(isbn,book_name,rating);
                answer.add(item);
            }
        }
        catch (SQLException e ){
            e.printStackTrace();
        }

        QueryResult.ResultQ8 [] res = answer.toArray(new QueryResult.ResultQ8[answer.size()]);
        return res;
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

        String sql;
        String query;
        ResultSet resultset = null;
        double sum_rating  = 0;
        keyword = "%" + keyword + "%";

        sql = "UPDATE book b\n" +
                "SET b.rating = b.rating +1\n" +
                "WHERE b.book_name LIKE (?) AND b.rating <= 4";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1,keyword);
            preparedStatement.executeUpdate();

            //System.out.println("my statement : " + sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        query = "SELECT SUM(b.rating) as sum_rating\n" +
                "FROM db2098796.book b";

        try{
            resultset = statement.executeQuery(query);
        }
        catch (SQLException e ){
            e.printStackTrace();
        }


        try{
            resultset.next();
            sum_rating = resultset.getDouble("sum_rating");

        }
        catch (SQLException e ){
            e.printStackTrace();
        }

        return sum_rating;

    }

    /**
     * Delete publishers in publisher table who haven't published a book yet.
     *
     * @return count of all rows of the publisher table after delete operation.
     */
    @Override
    public int function10() {
        String sql;
        String query;
        ResultSet resultset = null;
        int count_rows = 0;

        sql = "DELETE FROM db2098796.publisher p\n" +
                "WHERE p.publisher_id NOT IN (SELECT b.publisher_id FROM db2098796.book b)";

        try{
            statement.executeUpdate(sql);
        }
        catch (SQLException e ){
            e.printStackTrace();
        }

        query = "SELECT COUNT(*) as count_rows FROM db2098796.publisher p";

        try{
            resultset = statement.executeQuery(query);
        }
        catch (SQLException e ){
            e.printStackTrace();
        }

        try{
            resultset.next();
            count_rows = resultset.getInt("count_rows");

        }
        catch (SQLException e ){
            e.printStackTrace();
        }

        return count_rows;
    }
}
