<?php
namespace lib;

use PDO;
use PDOException;

/**
 * Class Db
 * @package lib
 */
class Db
{
    protected ?PDO $dbh = null;
    protected $query = null;
    protected $timestamp_writes = false;
    protected $prefix = null;

    /**
     * Соединение с базой данных
     *
     * @param string $driver
     * @param string $host
     * @param string $user
     * @param string $pass
     * @param string $name
     * @param string $charset
     * @param null   $prefix
     */
    public function __construct(string $driver = 'mysql', string $host = 'localhost', string $user = 'root', string $pass = '', string $name = 'dbname', string $charset = 'utf8', $prefix = null)
    {
        $dsn = $driver . ':host=' . $host;
        if (!empty($name))
            $dsn .= ';dbname=' . $name;

        if (!empty($prefix))
            $this->prefix = $prefix;

        $dsn .= ';charset=' . $charset;
        try
        {
            $this->dbh = new PDO(
                $dsn, $user, $pass, [
                    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
                    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC
                ]
            );
        } catch (PDOException $e) {
            error_log($e);
            return false;
        }
    }

    /**
     * @param string $sql
     *
     * @return bool
     */
    public function execute(string $sql)
    {
        $sth = $this->dbh->prepare($sql);
        return $sth->execute();
    }

    /**
     * @param string $query
     * @param array $params
     *
     * @return array
     */
    public function query(string $query, array $params = [])
    {
        $this->query = $this->dbh->prepare($query);
        $res = !empty($params) ? $this->query->execute($params) : $this->query->execute();
        return $res ? $this->query->fetchAll() : [];
    }

    /**
     * @param $database
     *
     * @return bool
     */
    public function useDatabase($database)
    {
        $sql_str     = 'USE ' . $database;
        $this->query = $this->dbh->prepare($sql_str);

        return $this->query->execute();
    }

    /**
     * @param $database
     *
     * @return bool
     */
    public function createDatabase($database)
    {
        $sql_str     = 'CREATE DATABASE IF NOT EXISTS ' . $database . ' DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;';
        $this->query = $this->dbh->prepare($sql_str);

        return $this->query->execute();
    }

    /**
     * @param $table
     *
     * @param array $columns
     * @return bool
     */
    public function createTable($table, array $columns)
    {
        $sql_str = 'CREATE TABLE IF NOT EXISTS ' . $this->prefix . $table . ' . (id INT(11) NOT NULL AUTO_INCREMENT ';
        foreach ($columns as $col_key => $col_val)
            $sql_str .= ', ' . $col_key . ' ' . $col_val;

        $sql_str .= ', PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;';
        $this->query = $this->dbh->prepare($sql_str);

        return $this->query->execute();
    }

    /**
     * Метод select
     *    - выборка данных из таблицы
     *
     * @param string|array $table    - имя таблицы
     * @param array|null $where    - массив фильтров выборки
     * @param int|null $limit    (optional) - количество строк
     * @param int|null $start    (optional) - строка, с которой начинаем
     * @param array $order_by (optional) - сортировка
     *
     * @return mixed - вернёт либо строку (массив строк), либо ошибку (false)
     *
     * Примеры запросов:
     * $pdo->select('user', ['id' => $id], 1) - выбрать только одну строку из user с нужным id
     * $pdo->select(['user', ['name']], ['id' => $id], 1) - выбрать одну строку из user с нужным id, получив только поле name
     * $pdo->select(['user', ['name', 'phone'], ['sex' => 0]) - выберет все строки, где sex 0, вернув поля name и phone
     */
    public function select($table, ?array $where = [], int $limit = null, int $start = null, array $order_by = [], $or = true)
    {
        $off = false;
        // Отправили обычный запрос
        if ($table == 'defSelect') {
            $off = true;
            $sql_str = $where[0]; // Запрос
            $where = $where[1] ?? null; // Параметры
        } else {
            // строим строку
            $sql_str = 'SELECT ';

            if (is_array($table)) {
                $sql_str .= !is_array($table[1]) ?
                    $table[1] . ' FROM ' :
                    implode(', ', $table[1]) . ' FROM ';
                $sql_str .= $this->prefix . $table[0];
            } else
                $sql_str .= ' * FROM ' . $this->prefix . $table;

            $add_and = false;

            if (!empty($where) and is_array($where)) {
                // добавляем where, если указано
                $sql_str .= ' WHERE ';
                foreach ($where as $key => $val) {

                    if ($add_and)
                        $sql_str .= $or ? ' AND ' : ' OR ';
                    else
                        $add_and = true;

                    // Если не =, а другие параметры
                    if (strpos($key, "|")) {
                        $key = explode("|", $key);
                        $sql_str .= $key[0] . ' ' . $key[1] . ' :' . $key[0];
                    } else
                        $sql_str .= $key . ' = :' . $key;
                }
            }
        }

            // добавляем order, если указано
            if (!empty($order_by)) {
                $sql_str .= ' ORDER BY ';
                $add_comma = false;
                foreach ($order_by as $column => $order) {
                    if ($add_comma)
                        $sql_str .= ', ';
                    else
                        $add_comma = true;

                    $sql_str .= $column . ' ' . $order;
                }
        }

        try
        {
            $pdoDriver = $this->dbh->getAttribute(PDO::ATTR_DRIVER_NAME);
            $disableLimit = ['sqlsrv', 'mssql', 'oci'];

            // добавляем limit, если указано
            if (!$off && !empty($limit) and !in_array($pdoDriver, $disableLimit))
                $sql_str .= ' LIMIT ' . ($start != null ? $start . ', ' : '') . $limit;

            $this->query = $this->dbh->prepare($sql_str);

            if (!empty($where) and is_array($where)) {
                // заменяем параметры
                foreach ($where as $key => $val) {
                    if (strpos($key, "|"))
                        $key = explode("|", $key)[0];
                    $this->query->bindValue(':' . $key, $val);
                }
            }

            $this->query->execute();

            // возвращаем результаты (только одну строку, если указано)
            if (!is_null($limit) and $limit == 1)
                return $this->query->fetch();
            else {
                $res = [];
                while ($row = $this->query->fetch())
                    $res[] = $row;

                return $res;
            }
        }
        catch (PDOException $e) {
            error_log($e);
            return false;
        }
    }

    /**
     * Метод selectCount - количество строк в таблице
     *
     * @param string|array $table   - имя таблицы
     * @param array|null $where     - массив фильтров выборки
     *
     * @return mixed - вернёт либо кол-во строк, либо ошибку (false)
     */
    public function selectCount($table, ?array $where = [])
    {
        if ($table == 'defSelect') {
            $sql_str = $where[0];
            $where = $where[1] ?? null;
        } else {
            // строим строку
            $sql_str = 'SELECT COUNT(';

            if (is_array($table)) {
                $sql_str .= !is_array($table[1]) ?
                    $table[1] . ') FROM ' :
                    implode(', ', $table[1]) . ') FROM ';
                $sql_str .= $this->prefix . $table[0];
            } else
                $sql_str .= '*) FROM ' . $this->prefix . $table;

            $add_and = false;

            if (!empty($where) and is_array($where)) {
                // добавляем where, если указано
                $sql_str .= ' WHERE ';
                foreach ($where as $key => $val) {
                    if ($add_and)
                        $sql_str .= ' AND ';
                    else
                        $add_and = true;

                    // Если не =, а другие параметры
                    if (strpos($key, "|")) {
                        $key = explode("|", $key);
                        $sql_str .= "`$key[0]` $key[1] :$key[0]";
                    } else
                        $sql_str .= "`$key` = :$key";
                }
            }
        }

        try
        {
            $this->query = $this->dbh->prepare($sql_str);

            if (!empty($where) and is_array($where)) {
                // заменяем параметры
                foreach ($where as $key => $val) {
                    if (strpos($key, "|"))
                        $key = explode("|", $key)[0];
                    $this->query->bindValue(':' . $key, $val);
                }
            }
            $this->query->execute();

            return $this->query->fetchColumn();
        }
        catch (PDOException $e)
        {
            error_log($e);
            return false;
        }
    }

    /**
     * Метод insert - добавить строку в базу
     *
     * @param string $table             - имя таблицы
     * @param array $params             - добавляемые параметры со значениями
     * @param bool|null $timestamp_this (Optional), если true, то настраиваем значения date_created and date_modified
     *
     * @return mixed - вернёт id добавленной строки или ошибку (false)
     */
    public function insert(string $table, array $params, bool $timestamp_this = null)
    {

        if ($table == 'defInsert') {
            $sql_str = $params[0];
            $params = $params[1];
        } else {

            if (is_null($timestamp_this))
                $timestamp_this = $this->timestamp_writes;

            $columns_str = ' (';
            $values_str = ' VALUES (';
            $add_comma = false;

            foreach ($params as $key => $val) {
                if ($add_comma) {
                    $columns_str .= ', ';
                    $values_str .= ', ';
                } else
                    $add_comma = true;

                $columns_str .= "`$key`";
                $values_str .= ':' . $key;
            }

            if ($timestamp_this === true) {
                $columns_str .= ($add_comma ? ', ' : '') . 'date_created, date_modified';
                $values_str .= ($add_comma ? ', ' : '') . time() . ', ' . time();
            }

            $columns_str .= ') ';
            $values_str .= ')';

            $sql_str = 'INSERT INTO ' . $this->prefix . $table . $columns_str . $values_str;
        }

        try
        {
            $this->query = $this->dbh->prepare($sql_str);
            foreach ($params as $key => $val)
            {
                if ($val === 'CURRENT_TIMESTAMP' || $val === 'NOW()')
                    $val = date('Y-m-d H:i:s');

                $this->query->bindValue(':' . $key, $val);
            }

            $this->query->execute();
            return $this->dbh->lastInsertId();

        }
        catch (PDOException $e) {
            error_log($e);
            return false;
        }

    }

    /**
     * Метод insertMultiple - добавить несколько строк в базу
     *
     * @param string $table           - имя таблицы
     * @param array $columns         - имена полей
     * @param array $rows            - параметры со значениями
     * @param bool|null $timestamp_these (Optional)
     *
     * @return mixed
     */
    public function insertMultiple(string $table, array $columns = [], array $rows = [], bool $timestamp_these = null)
    {
        if (is_null($timestamp_these))
            $timestamp_these = $this->timestamp_writes;

        if ($timestamp_these === true) {
            $columns[] = 'date_created';
            $columns[] = 'date_modified';
        }
        $columns_str = ' (' . implode(',', $columns) . ') ';

        $values_str = 'VALUES ';
        $add_comma  = false;

        foreach ($rows as $row_index => $row_values)
        {
            if ($add_comma)
                $values_str .= ', ';
            else
                $add_comma = true;

            $values_str          .= ' (';
            $add_comma_for_value = false;
            foreach ($row_values as $value_index => $value)
            {
                if ($add_comma_for_value)
                    $values_str .= ', ';
                else
                    $add_comma_for_value = true;

                $values_str .= ':' . $row_index . '_' . $value_index;
            }

            if ($timestamp_these)
                $values_str .= ($add_comma_for_value ? ', ' : '') . time() . ', ' . time();

            $values_str .= ')';
        }

        $sql_str = 'INSERT INTO ' . $this->prefix . $table . $columns_str . $values_str;

        try
        {
            $this->dbh->beginTransaction();
            $this->query = $this->dbh->prepare($sql_str);

            foreach ($rows as $row_index => $row_values)
            {
                foreach ($row_values as $value_index => $value)
                    $this->query->bindValue(':' . $row_index . '_' . $value_index, $value);
            }

            $this->query->execute();
            $this->dbh->commit();

            return true;
        }
        catch (PDOException $e) {
            $this->dbh->rollback();
            error_log($e);
            return false;
        }
    }


    /**
     * @return string - id последней вставленной записи
     */
    public function lastInsertId()
    {
        return $this->dbh->lastInsertId();
    }

    /**
     * Метод update - обновить строку в базе
     *
     * @param string $table             - имя таблицы
     * @param array|string  $params     - параметры со значениями
     * @param array $wheres (Optional)  - фильтры поиска строки
     * @param bool|null $timestamp_this (Optional)
     *
     * @return int|bool - вернёт кол-во обновлённых строк или ошибку (false)
     */
    public function update(string $table, $params, array $wheres = [], bool $timestamp_this = null)
    {
        $off = false;
        $set_string = '';
        if ($table == 'defUpdate')
            $off = true;
        else {
            if (is_null($timestamp_this))
                $timestamp_this = $this->timestamp_writes;

            $add_comma = false;
            foreach ($params as $key => $val) {
                if ($add_comma)
                    $set_string .= ', ';
                else
                    $add_comma = true;

                $set_string .= "`$key` = :param_$key";
            }

            if ($timestamp_this === true)
                $set_string .= ($add_comma ? ', ' : '') . 'date_modified=' . time();
        }

        $where_string = '';
        if (!empty($wheres)) {
            $where_array = [];
            foreach ($wheres as $key => $val)
                $where_array[] = "`$key` = :where_$key";

            $where_string = ' WHERE ' . implode(' AND ', $where_array);
        }

        $sql_str = $off ? $params . $where_string : 'UPDATE ' . $this->prefix . $table . ' SET ' . $set_string . $where_string;

        try
        {
            $this->query = $this->dbh->prepare($sql_str);

            if (!$off) {
                foreach ($params as $key => $val)
                $this->query->bindValue(':param_' . $key, $val);
            }

            foreach ($wheres as $key => $val)
                $this->query->bindValue(':where_' . $key, $val);

            $successful_update = $this->query->execute();

            return $successful_update ? $this->query->rowCount() : false;
        }
        catch (PDOException $e) {
            error_log($e);
            return false;
        }
    }

    /**
     * Метод delete.
     *    - удалить строки из таблицы
     *
     * @param $table  - имя таблицы
     * @param array $params - фильтры поиска нужных строк для удаления
     *
     * @return bool - true/false
     */
    public function delete($table, array $params = [])
    {
        if ($table == 'defDelete') {
            $sql_str = $params[0];
            $params = $params[1] ?? null;
        } else {
            $sql_str = 'DELETE FROM ' . $this->prefix . $table;
            $sql_str .= (count($params) > 0 ? ' WHERE ' : '');

            $add_and = false;
            foreach ($params as $key => $val) {
                if ($add_and)
                    $sql_str .= ' AND ';
                else
                    $add_and = true;

                // Если не =, а другие параметры
                if (strpos($key, "|")) {
                    $key = explode("|", $key);
                    $sql_str .= $key[0] . ' ' . $key[1] . ' :' . $key[0];
                } else
                    $sql_str .= $key . ' = :' . $key;
            }
        }

        try
        {
            $this->query = $this->dbh->prepare($sql_str);
            if ($params) {
                foreach ($params as $key => $val) {
                    if (strpos($key, "|"))
                        $key = explode("|", $key)[0];
                    $this->query->bindValue(':' . $key, $val);
                }
            }

            $successful_delete = $this->query->execute();

            return $successful_delete ? $this->query->rowCount() : false;
        }
        catch (PDOException $e)
        {
            error_log($e);
            return false;
        }
    }

    /**
     * @param $table
     *
     * @return bool
     */
    public function optimizeTable($table)
    {
        $sql_str     = 'OPTIMIZE TABLE ' . $this->prefix . $table . ';';
        $this->query = $this->dbh->prepare($sql_str);

        return $this->query->execute();
    }

    /**
     * @param $table
     *
     * @return bool
     */
    public function truncateTable($table)
    {
        $sql_str     = 'TRUNCATE TABLE ' . $this->prefix . $table . ';';
        $this->query = $this->dbh->prepare($sql_str);

        return $this->query->execute();
    }

    /**
     * @param $table
     *
     * @return bool
     */
    public function dropTable($table)
    {
        $sql_str     = 'DROP TABLE IF EXISTS ' . $this->prefix . $table;
        $this->query = $this->dbh->prepare($sql_str);

        return $this->query->execute();
    }
}
