package fr.xilitra.mysqldb;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

public class SQLConnection {

    private HikariDataSource pool = null;
    private String host, port, database, username, password;
    private Logger logs = Logger.getLogger("SQLConnection");

    public SQLConnection(String host, String port, String database, String username, String password) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
    }

    public void initConnectionMysql() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://" + this.host + ":" + this.port + "/" + this.database
                        + "?verifyServerCertificate=false"
                        + "&useSSL=false"
                        + "&serverTimezone=UTC"
                        + "&characterEncoding=UTF-8"
                        + "&jdbcCompliantTruncation=false"
                //+ "&allowMultiQueries=true"
        );
        config.setUsername(this.username);
        config.setPassword(this.password);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("useLocalSessionState", true);
        config.addDataSourceProperty("rewriteBatchedStatements", true);
        config.addDataSourceProperty("cacheResultSetMetadata", true);
        config.addDataSourceProperty("cacheServerConfiguration", true);
        config.addDataSourceProperty("elideSetAutoCommits", true);
        config.addDataSourceProperty("maintainTimeStats", false);

        // Avoid maxLifeTime disconnection
        config.setMinimumIdle(0);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(35000);
        config.setMaxLifetime(45000);

        this.pool = new HikariDataSource(config);
        this.logs.info("Connected to MySQL with HikariCP!");
    }

    public void initConnectionMariaDB() {
        try {
            Class.forName("org.mariadb.jdbc.MariaDbDataSource");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }
        HikariConfig config = new HikariConfig();

        config.setDataSourceClassName("org.mariadb.jdbc.MariaDbDataSource");
        config.addDataSourceProperty("url", "jdbc:mariadb://" + this.host + ":" + this.port + "/" + this.database);
        config.addDataSourceProperty("user", this.username);
        config.addDataSourceProperty("password",this.password);

        config.setMinimumIdle(0);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(35000);
        config.setMaxLifetime(45000);

        this.pool = new HikariDataSource(config);
        this.logs.info("Connected to MySQL with HikariCP!");
    }

    public boolean isConnected() {
        return this.pool != null && !this.pool.isClosed();
    }

    public void closeConnection() {
        this.pool.close();
        this.pool = null;
    }

    private void closeRessources(ResultSet rs, PreparedStatement st) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (st != null) {
            try {
                st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private PreparedStatement prepareStatement(Connection conn, String query, Object... vars) {
        try {
            PreparedStatement ps = conn.prepareStatement(query);
            int i = 0;
            if (query.contains("?") && vars.length != 0) {
                for (Object obj : vars) {
                    i++;
                    ps.setObject(i, obj);
                }
            }
            return ps;

        } catch (SQLException exception) {
            this.logs.severe("MySQL error: " + exception.getMessage());
        }

        return null;
    }

    public void AsyncQuery(final String query, final Callback<SQLRowSet> callback, final Object... vars) {
        Scheduler.runTask(() -> {
            try (Connection conn = this.pool.getConnection()) {
                try (PreparedStatement ps = this.prepareStatement(conn, query, vars)) {
                    assert ps != null;
                    try (ResultSet rs = ps.executeQuery()) {
                        SQLRowSet SQLRowSet = new SQLRowSet(rs);
                        this.closeRessources(rs, ps);
                        if (callback != null) {
                            callback.run(SQLRowSet);
                        }
                    }
                } catch (SQLException e) {
                    this.logs.severe("MySQL error: " + e.getMessage());
                    e.printStackTrace();
                }
            } catch (SQLException exception) {
                this.logs.severe("Error when getting pool connection !");
                exception.printStackTrace();
            }
        });
    }

    public SQLRowSet query(final String query, final Object... vars) {
        try (Connection conn = this.pool.getConnection()) {
            try (PreparedStatement ps = this.prepareStatement(conn, query, vars)) {
                assert ps != null;
                try (ResultSet rs = ps.executeQuery()) {
                    SQLRowSet SQLRowSet = new SQLRowSet(rs);
                    this.closeRessources(rs, ps);
                    return SQLRowSet;
                }
            } catch (SQLException e) {
                this.logs.severe("MySQL error: " + e.getMessage());
                e.printStackTrace();
            }
        } catch (SQLException exception) {
            this.logs.severe("Error when getting pool connection !");
            exception.printStackTrace();
        }
        return null;
    }

    public void query(final String query, final Callback<ResultSet> callback, final Object... vars) {
        try (Connection conn = this.pool.getConnection()) {
            try (PreparedStatement ps = this.prepareStatement(conn, query, vars)) {
                assert ps != null;
                try (ResultSet rs = ps.executeQuery()) {
                    callback.run(rs);
                    this.closeRessources(rs, ps);
                }
            } catch (SQLException e) {
                this.logs.severe("MySQL error: " + e.getMessage());
                e.printStackTrace();
            }
        } catch (SQLException exception) {
            this.logs.severe("Error when getting pool connection !");
            exception.printStackTrace();
        }
    }

    public void AsyncExecuteCallback(final String query, final Callback<Integer> callback, final Object... vars) {
        Scheduler.runTask(() -> {
            try (Connection conn = this.pool.getConnection()) {
                try (PreparedStatement ps = this.prepareStatement(conn, query, vars)) {
                    assert ps != null;
                    ps.execute();
                    this.closeRessources(null, ps);
                    if (callback != null) {
                        callback.run(-1);
                    }
                } catch (SQLException exception) {
                    if (exception.getErrorCode() == 1060) {
                        return;
                    }
                    this.logs.severe("MySQL error: " + exception.getMessage());
                    exception.printStackTrace();
                }
            } catch (SQLException exception) {
                this.logs.severe("Error when getting pool connection !");
                exception.printStackTrace();
            }
        });
    }

    public void AsyncExecute(final String query, final Object... vars) {
        this.AsyncExecuteCallback(query, null, vars);
    }

    public void execute(final String query, final Object... vars) {
        try (Connection conn = this.pool.getConnection()) {
            try (PreparedStatement ps = this.prepareStatement(conn, query, vars)) {
                assert ps != null;
                ps.execute();
                this.closeRessources(null, ps);
            } catch (SQLException exception) {
                this.logs.severe("MySQL error: " + exception.getMessage());
                exception.printStackTrace();
            }
        } catch (SQLException exception) {
            this.logs.severe("Error when getting pool connection !");
            exception.printStackTrace();
        }
    }

}
