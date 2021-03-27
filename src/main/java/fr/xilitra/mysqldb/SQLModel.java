package fr.xilitra.mysqldb;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;

public abstract class SQLModel {

    private final String table;

    private final String primaryKey;


    HashMap<String, Object> columns = new HashMap<>();

    private boolean populate = false;

    public SQLModel(String table, String primaryKey) {
        this.table = table;
        this.primaryKey = primaryKey;
    }

    public String getTable() {
        return this.table;
    }

    public String getPrimaryKey() {
        return this.primaryKey;
    }

    public HashMap<String, Object> getColumns() {
        return this.columns;
    }

    public void populate(ResultSet resultSet) {
        try {
            ResultSetMetaData meta = resultSet.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); ++i) {
                this.columns.put(meta.getColumnName(i), resultSet.getObject(i));
            }
            this.populate = true;
            this.onPopulated();
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }

    protected void onPopulated() {}

    public Object get(String columnName) {
        return this.columns.get(columnName);
    }

    public String getString(String columnName) {
        return (String) this.columns.get(columnName);
    }

    public int getInt(String columnName) {
        return Integer.parseInt(this.columns.get(columnName).toString());
    }

    public double getDouble(String columnName) {
        return Double.parseDouble(this.columns.get(columnName).toString());
    }

    public long getLong(String columnName) {
        return Long.parseLong(this.columns.get(columnName).toString());
    }

    public void set(String columnName, Object value) {
        if (this.exists() && columnName.equals(this.primaryKey)) {
            return;
        }
        this.columns.put(columnName, value);
        if (!this.populate) {
            return;
        }
        if (value == null) {
            MySqlDB.getDB().AsyncExecute("UPDATE " + this.table
                    + " SET " + columnName + " = NULL WHERE " + this.primaryKey + " = ?", this.getInt(this.primaryKey));
        } else {
            MySqlDB.getDB().AsyncExecute("UPDATE " + this.table
                    + " SET " + columnName + " = ? WHERE " + this.primaryKey + " = ?", value, this.getInt(this.primaryKey));
        }
    }

    public void setSync(String columnName, Object value) {
        if (this.exists() && columnName.equals(this.primaryKey)) {
            return;
        }
        this.columns.put(columnName, value);
        if (value == null) {
            MySqlDB.getDB().execute("UPDATE " + this.table
                    + " SET " + columnName + " = NULL WHERE " + this.primaryKey + " = ?", this.getInt(this.primaryKey));
        } else {
            System.out.println("UPDATE " + this.table
                    + " SET " + columnName + " = " + value + " WHERE " + this.primaryKey + " = " + this.getInt(this.primaryKey));
            MySqlDB.getDB().execute("UPDATE " + this.table
                    + " SET " + columnName + " = ? WHERE " + this.primaryKey + " = ?", value, this.getInt(this.primaryKey));
        }
    }

    public void add(String columnName, int add) {
        if (this.exists() && columnName.equals(this.primaryKey)) {
            return;
        }
        if (!this.populate) {
            return;
        }
        this.columns.put(columnName, this.getInt(columnName) + add);
        MySqlDB.getDB().AsyncExecute("UPDATE " + this.table
                + " SET " + columnName + " = " + columnName + " + " + add
                + " WHERE " + this.primaryKey + " = ?", this.getInt(this.primaryKey));
    }

    public void sub(String columnName, int sub) {
        if (this.exists() && columnName.equals(this.primaryKey)) {
            return;
        }
        if (!this.populate) {
            return;
        }
        this.columns.put(columnName, this.getInt(columnName) - sub);
        MySqlDB.getDB().AsyncExecute("UPDATE " + this.table
                + " SET " + columnName + " = " + columnName + " - " + sub
                + " WHERE " + this.primaryKey + " = ?", this.getInt(this.primaryKey));
    }

    public boolean exists() {
        return this.populate;
    }

}
