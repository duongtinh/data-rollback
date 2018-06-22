import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;

/**
 * Created by mint on 5/3/17.
 */
public class Main {

    public static void main(String[] args) {
        Tuple2<Connection, Boolean> ret = mysqlBatchInsert();
        if (ret._2) {
            if (cassandraBatchInsert()) {
                System.out.println("success");
                try {
                    ret._1.setAutoCommit(true);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    ret._1.rollback();
                    ret._1.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static Boolean cassandraBatchInsert() {
        String username = "mint";
        String password = "mint@123";
        String host = "localhost";
        String keyspace = "keyspace1";
        PlainTextAuthProvider auth = new PlainTextAuthProvider(username, password);
        Cluster cluster = Cluster.builder().withAuthProvider(auth).addContactPoint(host).build();
        Session session = cluster.connect(keyspace);

        Batch batch = QueryBuilder.batch();

        try {
            for (int i = 0; i < 100; i++) {
                UUID uid = UUID.randomUUID();
                Insert insert = insertInto("table2").values(
                        new String[]{"id", "from_user", "to_user", "time", "text"},
                        new Object[]{uid, "from", "to", "time", "text"});
                // is this the right way to set consistency level for Batch?
                insert.setConsistencyLevel(ConsistencyLevel.QUORUM);
                batch.add(insert);
            }
            Insert insert = insertInto("table2").values(
                    new String[]{"id", "from_user", "to_user", "time", "text"},
                    new Object[]{null, "from_from", "to_to", null, "text_text"});
            // is this the right way to set consistency level for Batch?
            insert.setConsistencyLevel(ConsistencyLevel.QUORUM);
            batch.add(insert);

            session.execute(batch);

            System.out.println("success");
            session.close();

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            session.close();
            cluster.close();
            return false;
        }
    }

    public static Tuple2<Connection, Boolean> mysqlBatchInsert() {
        Connection connection = null;
        try {
            String query = "INSERT INTO table1(id,name) value(?,?);";
            connection = DriverManager.getConnection("jdbc:mysql://localhost/db?user=root&password=root&serverTimezone=UTC");
            connection.setAutoCommit(false);
            connection.setSavepoint();
            PreparedStatement ps = connection.prepareStatement(query);
            for (int i = 0; i < 100; i++) {
                ps.setString(1, "id1");
                ps.setString(2, "name1");
                ps.addBatch();
            }
            ps.executeBatch();

            return new Tuple2(connection, true);
        } catch (Exception e) {
            try {
                connection.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
            return new Tuple2(connection, false);
        }
    }
}
