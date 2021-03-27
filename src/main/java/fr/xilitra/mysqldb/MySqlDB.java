package fr.xilitra.mysqldb;

import fr.xilitra.mysqldb.exception.DBException;

import java.util.HashMap;

public class MySqlDB {

    private static HashMap<String, SQLConnection> connections= new HashMap<>();
    private static SQLConnection mainDBConnection;

    public static void setupMySqlDb(String name, SQLConnection sqlConnection) {

        SQLConnection connection = sqlConnection;
        connection.initConnectionMysql();
        connections.put(name, connection);    }

    public static void setupMariaDb(String name, SQLConnection sqlConnection){
        SQLConnection connection = sqlConnection;
        connection.initConnectionMariaDB();
        connections.put(name, connection);
    }

    public static SQLConnection getDB() {
        return mainDBConnection;
    }

    public static void selectDB(String name) throws DBException {
        if(!connections.containsKey(name)){
            throw new DBException();
        }

        mainDBConnection =  connections.get(name);
    }
}
