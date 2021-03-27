package fr.xilitra.mysqldb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class SQLModels<T extends SQLModel> {

    private Class<T> method;
    private Logger logs;

    public SQLModels(Class<T> method) {
        this.method = method;
        this.logs = Logger.getLogger("SQLModels<" + method.getSimpleName() + ">");
    }

    public T get(int primaryKey) {
        try {
            T model = this.method.newInstance();
            this.get(model, primaryKey);
            return model;
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void get(T model, int primaryKey) {
        assert model != null;
        MySqlDB.getDB().query("SELECT * FROM " + model.getTable()
                + " WHERE " + model.getPrimaryKey() + " = " + primaryKey, resultSet -> {
            try {
                if (resultSet.first()) {
                    model.populate(resultSet);
                }
            } catch (SQLException exception) {
                exception.printStackTrace();
                this.logs.severe("Error SQL get() = " + exception.getMessage());
            }
        });
    }

    public T getFirst(String query, Object... vars) {
        ArrayList<T> results = this.get(query, vars);
        return results.size() > 0 ? results.get(0) : null;
    }

    public ArrayList<T> get(String query, Object... vars) {
        ArrayList<T> results = new ArrayList<>();
        try {
            T model = this.method.newInstance();
            assert model != null;

            MySqlDB.getDB().query("SELECT * FROM " + model.getTable() + (query != null ? " " + query : ""),
                    resultSet -> {
                        try {
                            while (resultSet.next()) {
                                T newModel = this.method.newInstance();
                                assert newModel != null;
                                newModel.populate(resultSet);
                                results.add(newModel);
                            }
                        } catch (Exception exception) {
                            exception.printStackTrace();
                        }
                    }, vars
            );
        } catch (Exception exception) {
            exception.printStackTrace();
            this.logs.severe("Error SQL get() #2 = " + exception.getMessage());
        }
        return results;
    }

    public ArrayList<T> all() {
        return this.get(null);
    }

    public T getOrInsert(int primaryKey) {
        return this.getOrInsert(new HashMap<>(), primaryKey);
    }

    public T getOrInsert(HashMap<String, Object> defaultValues, int primaryKey) {
        try {
            T model = this.method.newInstance();
            this.getOrInsert(model, defaultValues, primaryKey);
            return model;
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            this.logs.severe("Error SQL getOrInsert() = " + e.getMessage());
        }
        return null;
    }

    public void getOrInsert(T model, int primaryKey) {
        this.getOrInsert(model, null, primaryKey);
    }

    public void getOrInsert(T model, HashMap<String, Object> defaultValues, int primaryKey) {
        try {
            this.get(model, primaryKey);
            if (!model.exists()) {
                model.set(model.getPrimaryKey(), primaryKey);
                if (defaultValues != null) {
                    for (Map.Entry<String, Object> entry : defaultValues.entrySet()) {
                        model.set(entry.getKey(), entry.getValue());
                    }
                }
                this.insert(model);
                this.get(model, primaryKey);
            }
        } catch (Exception exception) {
            exception.printStackTrace();
            this.logs.severe("Error SQL getOrInsert() #2 = " + exception.getMessage());
        }
    }

    public T getOrInsert(HashMap<String, Object> defaultValues, String query, Object... vars) {
        try {
            T foundRow = this.getFirst(query, vars);
            if (foundRow != null) {
                return foundRow;
            }
            T model = this.method.newInstance();
            if (defaultValues != null) {
                for (Map.Entry<String, Object> entry : defaultValues.entrySet()) {
                    model.set(entry.getKey(), entry.getValue());
                }
            }
            this.insert(model);
            return this.getOrInsert(defaultValues, query, vars);
        } catch (Exception exception) {
            exception.printStackTrace();
            this.logs.severe("Error SQL getOrInsert() #3 = " + exception.getMessage());
        }
        return null;
    }

    public void delete(String query, Object... vars) {
        try {
            T model = this.method.newInstance();

            String queryString = "DELETE FROM " + model.getTable() + " " + query;
            this.logs.info(queryString);
            MySqlDB.getDB().execute(queryString, vars);
        }
        catch (Exception e) {}
    }

    public void insert(T model) {
        StringBuilder set = new StringBuilder();
        if (model.get(model.getPrimaryKey()) != null) {
            set.append("`").append(model.getPrimaryKey()).append("` = ").append(model.getInt(model.getPrimaryKey()));
        }
        for (Map.Entry<String, Object> entry : model.getColumns().entrySet()) {
            if (entry.getKey().equals(model.getPrimaryKey())) {
                continue;
            }
            if (set.length() > 0) {
                set.append(", ");
            }
            if (entry.getValue() == null) {
                set.append("`").append(entry.getKey()).append("` = NULL");
            } else {
                String tmp = entry.getValue().toString().replaceAll("'", "''");
                set.append("`").append(entry.getKey()).append("` = '").append(tmp).append("'");
            }
        }
        String query = "INSERT INTO " + model.getTable()
                + " SET " + set.toString();
        this.logs.info(query);
        MySqlDB.getDB().execute(query);
    }
}
