package fr.xilitra.mysqldb;

public interface Callback<T> {

    void run(T response);
}
