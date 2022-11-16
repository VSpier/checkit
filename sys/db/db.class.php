<?php

namespace lib;

use Closure;
use PDO;
use PDOException;

/**
 * PDO Class
 */
class DB implements DbInterface
{
    /**
     * @var PDO|null
     */
    public ?PDO $pdo = null;

    /**
     * @var mixed Query variables
     */
    protected $select = '*';
    protected $from = null;
    protected $where = null;
    protected $limit = null;
    protected $offset = null;
    protected $join = null;
    protected $orderBy = null;
    protected $groupBy = null;
    protected $having = null;
    protected $grouped = false;
    protected $numRows = 0;
    protected $insertId = null;
    protected $query = null;
    protected $error = null;
    protected $result = [];
    protected $prefix = null;

    /**
     * @var array SQL operators
     */
    protected array $operators = ['=', '!=', '<', '>', '<=', '>=', '<>'];

    /**
     * @var Cache|null
     */
    protected ?Cache $cache = null;

    /**
     * @var string|null Cache Directory
     */
    protected $cacheDir = null;

    /**
     * @var int Total query count
     */
    protected int $queryCount = 0;

    /**
     * @var bool
     */
    protected $debug = true;

    /**
     * @var int Total transaction count
     */
    protected int $transactionCount = 0;

    /**
     * @param array $config
     */
    public function __construct(array $config)
    {
        $config['driver'] = $config['driver'] ?? 'mysql';
        $config['host'] = $config['host'] ?? 'localhost';
        $config['charset'] = $config['charset'] ?? 'utf8mb4';
        $config['collation'] = $config['collation'] ?? 'utf8mb4_general_ci';
        $config['port'] = $config['port'] ?? (strstr($config['host'], ':') ? explode(':', $config['host'])[1] : '');
        $this->prefix = $config['prefix'] ?? '';
        $this->cacheDir = $config['cachedir'] ?? __DIR__ . '/cache/';
        $this->debug = $config['debug'] ?? true;

        $dsn = '';

        /* 
        Поддержка: mysql, pgsql, sqlite
        */
        if (in_array($config['driver'], ['', 'mysql', 'pgsql'])) {
            $dsn = $config['driver'] . ':host=' . str_replace(':' . $config['port'], '', $config['host']) . ';'
                . ($config['port'] !== '' ? 'port=' . $config['port'] . ';' : '')
                . 'dbname=' . $config['database'];
        } elseif ($config['driver'] === 'sqlite')
            $dsn = 'sqlite:' . $config['database'];

        try {
            $this->pdo = new PDO($dsn, $config['username'], $config['password'], $config['options'] ?? null);
            $this->pdo->exec("SET NAMES '" . $config['charset'] . "' COLLATE '" . $config['collation'] . "'");
            $this->pdo->exec("SET CHARACTER SET '" . $config['charset'] . "'");
            $this->pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_OBJ);
        } catch (PDOException $e) {
            die('Ошибка подключения к базе данных: ' . $e->getMessage());
        }

        return $this->pdo;
    }

    /**
     * @param $table
     *
     * @return $this
     */
    public function table($table): DB
    {
        if (is_array($table)) {
            $from = '';
            foreach ($table as $key)
                $from .= $this->prefix . $key . ', ';

            $this->from = rtrim($from, ', ');
        } else {
            if (strpos($table, ',') > 0) {
                $tables = explode(',', $table);
                foreach ($tables as $key => &$value)
                    $value = $this->prefix . ltrim($value);

                $this->from = implode(', ', $tables);
            } else
                $this->from = $this->prefix . $table;
        }

        return $this;
    }

    /**
     * @param array|string $fields
     *
     * @return $this
     */
    public function select($fields): DB
    {
        $select = is_array($fields) ? implode(', ', $fields) : $fields;
        $this->optimizeSelect($select);

        return $this;
    }

    /**
     * @param string      $field
     * @param string|null $name
     *
     * @return $this
     */
    public function count(string $field, string $name = null): DB
    {
        $column = 'COUNT(' . $field . ')' . (!is_null($name) ? ' AS ' . $name : '');
        $this->optimizeSelect($column);

        return $this;
    }

    /**
     * @param string      $table
     * @param string|null $field1
     * @param string|null $operator
     * @param string|null $field2
     * @param string      $type
     *
     * @return $this
     */
    public function join(string $table, string $field1 = null, string $operator = null, string $field2 = null, string $type = ''): DB
    {
        $on = $field1;
        $table = $this->prefix . $table;

        if (!is_null($operator)) {
            $on = !in_array($operator, $this->operators)
                ? $field1 . ' = ' . $operator . (!is_null($field2) ? ' ' . $field2 : '')
                : $field1 . ' ' . $operator . ' ' . $field2;
        }

        $this->join = (is_null($this->join))
            ? ' ' . $type . 'JOIN' . ' ' . $table . ' ON ' . $on
            : $this->join . ' ' . $type . 'JOIN' . ' ' . $table . ' ON ' . $on;

        return $this;
    }

    /**
     * @param string $table
     * @param string $field1
     * @param string $operator
     * @param string $field2
     *
     * @return $this
     */
    public function innerJoin(string $table, string $field1, string $operator = '', string $field2 = ''): DB
    {
        return $this->join($table, $field1, $operator, $field2, 'INNER ');
    }

    /**
     * @param string $table
     * @param string $field1
     * @param string $operator
     * @param string $field2
     *
     * @return $this
     */
    public function leftJoin(string $table, string $field1, string $operator = '', string $field2 = ''): DB
    {
        return $this->join($table, $field1, $operator, $field2, 'LEFT ');
    }

    /**
     * @param string $table
     * @param string $field1
     * @param string $operator
     * @param string $field2
     *
     * @return $this
     */
    public function rightJoin(string $table, string $field1, string $operator = '', string $field2 = ''): DB
    {
        return $this->join($table, $field1, $operator, $field2, 'RIGHT ');
    }

    /**
     * @param string $table
     * @param string $field1
     * @param string $operator
     * @param string $field2
     *
     * @return $this
     */
    public function fullOuterJoin(string $table, string $field1, string $operator = '', string $field2 = ''): DB
    {
        return $this->join($table, $field1, $operator, $field2, 'FULL OUTER ');
    }

    /**
     * @param string $table
     * @param string $field1
     * @param string $operator
     * @param string $field2
     *
     * @return $this
     */
    public function leftOuterJoin(string $table, string $field1, string $operator = '', string $field2 = ''): DB
    {
        return $this->join($table, $field1, $operator, $field2, 'LEFT OUTER ');
    }

    /**
     * @param string $table
     * @param string $field1
     * @param string $operator
     * @param string $field2
     *
     * @return $this
     */
    public function rightOuterJoin(string $table, string $field1, string $operator = '', string $field2 = ''): DB
    {
        return $this->join($table, $field1, $operator, $field2, 'RIGHT OUTER ');
    }

    /**
     * @param array|string $where
     * @param string|array|null $operator
     * @param string|null $val
     * @param string $type
     * @param string $andOr
     *
     * @return $this
     */
    public function where($where, $operator = null, string $val = null, string $type = '', string $andOr = 'AND'): DB
    {
        if (is_array($where) && !empty($where)) {
            $_where = [];
            foreach ($where as $column => $data)
                $_where[] = $type . $column . '=' . $this->escape($data);

            $where = implode(' ' . $andOr . ' ', $_where);
        } else {
            if (is_null($where) || empty($where))
                return $this;

            if (is_array($operator)) {
                $params = explode('?', $where);
                $_where = '';
                foreach ($params as $key => $value) {
                    if (!empty($value))
                        $_where .= $type . $value . (isset($operator[$key]) ? $this->escape($operator[$key]) : '');
                }
                $where = $_where;
            } elseif (!in_array($operator, $this->operators) || !$operator)
                $where = $type . $where . ' = ' . $this->escape($operator);
            else
                $where = $type . $where . ' ' . $operator . ' ' . $this->escape($val);
        }

        if ($this->grouped) {
            $where = '(' . $where;
            $this->grouped = false;
        }

        $this->where = is_null($this->where)
            ? $where
            : $this->where . ' ' . $andOr . ' ' . $where;

        return $this;
    }

    /**
     * @param array|string $where
     * @param string|null  $operator
     * @param string|null  $val
     *
     * @return $this
     */
    public function orWhere($where, string $operator = null, string $val = null): DB
    {
        return $this->where($where, $operator, $val, '', 'OR');
    }

    /**
     * @param array|string $where
     * @param string|null  $operator
     * @param string|null  $val
     *
     * @return $this
     */
    public function notWhere($where, string $operator = null, string $val = null): DB
    {
        return $this->where($where, $operator, $val, 'NOT ', 'AND');
    }

    /**
     * @param array|string $where
     * @param string|null  $operator
     * @param string|null  $val
     *
     * @return $this
     */
    public function orNotWhere($where, string $operator = null, string $val = null): DB
    {
        return $this->where($where, $operator, $val, 'NOT ', 'OR');
    }

    /**
     * @param string $where
     * @param bool $not
     *
     * @return $this
     */
    public function whereNull(string $where, bool $not = false): DB
    {
        $where = $where . ' IS ' . ($not ? 'NOT' : '') . ' NULL';
        $this->where = is_null($this->where) ? $where : $this->where . ' ' . 'AND ' . $where;

        return $this;
    }

    /**
     * @param string $where
     *
     * @return $this
     */
    public function whereNotNull(string $where): DB
    {
        return $this->whereNull($where, true);
    }

    /**
     * @param Closure $obj
     *
     * @return $this
     */
    public function grouped(Closure $obj): DB
    {
        $this->grouped = true;
        call_user_func_array($obj, [$this]);
        $this->where .= ')';

        return $this;
    }

    /**
     * @param string $field
     * @param array  $keys
     * @param string $type
     * @param string $andOr
     *
     * @return $this
     */
    public function in(string $field, array $keys, string $type = '', string $andOr = 'AND'): DB
    {
        $_keys = [];
        foreach ($keys as $k => $v)
            $_keys[] = is_numeric($v) ? $v : $this->escape($v);

        $where = $field . ' ' . $type . 'IN (' . implode(', ', $_keys) . ')';

        if ($this->grouped) {
            $where = '(' . $where;
            $this->grouped = false;
        }

        $this->where = is_null($this->where)
            ? $where
            : $this->where . ' ' . $andOr . ' ' . $where;

        return $this;
    }

    /**
     * @param string $field
     * @param array  $keys
     *
     * @return $this
     */
    public function notIn(string $field, array $keys): DB
    {
        return $this->in($field, $keys, 'NOT ', 'AND');
    }

    /**
     * @param string $field
     * @param array  $keys
     *
     * @return $this
     */
    public function orIn(string $field, array $keys): DB
    {
        return $this->in($field, $keys, '', 'OR');
    }

    /**
     * @param string $field
     * @param array  $keys
     *
     * @return $this
     */
    public function orNotIn(string $field, array $keys): DB
    {
        return $this->in($field, $keys, 'NOT ', 'OR');
    }

    /**
     * @param string $field
     * @param string $data
     * @param string $type
     * @param string $andOr
     *
     * @return $this
     */
    public function like(string $field, string $data, string $type = '', string $andOr = 'AND'): DB
    {
        $like = $this->escape($data);
        $where = $field . ' ' . $type . 'LIKE ' . $like;

        if ($this->grouped) {
            $where = '(' . $where;
            $this->grouped = false;
        }

        $this->where = is_null($this->where)
            ? $where
            : $this->where . ' ' . $andOr . ' ' . $where;

        return $this;
    }

    /**
     * @param string $field
     * @param string $data
     *
     * @return $this
     */
    public function orLike(string $field, string $data): DB
    {
        return $this->like($field, $data, '', 'OR');
    }

    /**
     * @param string $field
     * @param string $data
     *
     * @return $this
     */
    public function notLike(string $field, string $data): DB
    {
        return $this->like($field, $data, 'NOT ', 'AND');
    }

    /**
     * @param string $field
     * @param string $data
     *
     * @return $this
     */
    public function orNotLike(string $field, string $data): DB
    {
        return $this->like($field, $data, 'NOT ', 'OR');
    }

    /**
     * @param int      $limit
     * @param int|null $limitEnd
     *
     * @return $this
     */
    public function limit(int $limit, int $limitEnd = null): DB
    {
        $this->limit = !is_null($limitEnd)
            ? $limit . ', ' . $limitEnd
            : $limit;

        return $this;
    }

    /**
     * @param int $offset
     *
     * @return $this
     */
    public function offset(int $offset): DB
    {
        $this->offset = $offset;

        return $this;
    }

    /**
     * @param int $perPage
     * @param int $page
     *
     * @return $this
     */
    public function pagination(int $perPage, int $page): DB
    {
        $this->limit = $perPage;
        $this->offset = (($page > 0 ? $page : 1) - 1) * $perPage;

        return $this;
    }

    /**
     * @param string      $orderBy
     * @param string|null $orderDir
     *
     * @return $this
     */
    public function orderBy(string $orderBy, string $orderDir = null): DB
    {
        if (!is_null($orderDir))
            $this->orderBy = $orderBy . ' ' . strtoupper($orderDir);
        else {
            $this->orderBy = stristr($orderBy, ' ') || strtolower($orderBy) === 'rand()'
                ? $orderBy
                : $orderBy . ' ASC';
        }

        return $this;
    }

    /**
     * @param string|array $groupBy
     *
     * @return $this
     */
    public function groupBy($groupBy): DB
    {
        $this->groupBy = is_array($groupBy) ? implode(', ', $groupBy) : $groupBy;

        return $this;
    }

    /**
     * @param string            $field
     * @param string|array|null $operator
     * @param string|null       $val
     *
     * @return $this
     */
    public function having(string $field, $operator = null, string $val = null): DB
    {
        if (is_array($operator)) {
            $fields = explode('?', $field);
            $where = '';
            foreach ($fields as $key => $value) {
                if (!empty($value))
                    $where .= $value . (isset($operator[$key]) ? $this->escape($operator[$key]) : '');
            }
            $this->having = $where;
        } elseif (!in_array($operator, $this->operators))
            $this->having = $field . ' > ' . $this->escape($operator);
        else
            $this->having = $field . ' ' . $operator . ' ' . $this->escape($val);

        return $this;
    }

    /**
     * @return int
     */
    public function numRows(): int
    {
        return $this->numRows;
    }

    /**
     * @return int|null
     */
    public function insertId(): ?int
    {
        return $this->insertId;
    }

    /**
     * @throw PDOException
     */
    public function error()
    {
        if ($this->debug === true) {
            if (php_sapi_name() === 'cli')
                die("Query: " . $this->query . PHP_EOL . "Error: " . $this->error . PHP_EOL);

            $msg = '<h1>Database Error</h1>';
            $msg .= '<h4>Query: <em style="font-weight:normal">"' . $this->query . '"</em></h4>';
            $msg .= '<h4>Error: <em style="font-weight:normal">' . $this->error . '</em></h4>';
            die($msg);
        }

        throw new PDOException($this->error . '. (' . $this->query . ')');
    }

    /**
     * @param string|bool $type
     * @param string|null $argument
     *
     * @return mixed
     */
    public function get($type = null, $argument = null)
    {
        $this->limit = 1;
        $query = $this->getAll(true);
        return $type === true ? $query : $this->query($query, false, $type, $argument);
    }

    /**
     * @param bool|string $type
     * @param string|null $argument
     *
     * @return mixed
     */
    public function getAll($type = null, $argument = null)
    {
        $query = 'SELECT ' . $this->select . ' FROM ' . $this->from;

        if (!is_null($this->join))
            $query .= $this->join;

        if (!is_null($this->where))
            $query .= ' WHERE ' . $this->where;

        if (!is_null($this->groupBy))
            $query .= ' GROUP BY ' . $this->groupBy;

        if (!is_null($this->having))
            $query .= ' HAVING ' . $this->having;

        if (!is_null($this->orderBy))
            $query .= ' ORDER BY ' . $this->orderBy;

        if (!is_null($this->limit))
            $query .= ' LIMIT ' . $this->limit;

        if (!is_null($this->offset))
            $query .= ' OFFSET ' . $this->offset;


        return $type === true ? $query : $this->query($query, true, $type, $argument);
    }

    /**
     * @param array $data
     * @param bool  $type
     *
     * @return bool|string|int|null
     */
    public function insert(array $data, $type = false)
    {
        $query = 'INSERT INTO ' . $this->from;

        $values = array_values($data);
        if (isset($values[0]) && is_array($values[0])) {
            $column = implode(', ', array_keys($values[0]));
            $query .= ' (' . $column . ') VALUES ';
            foreach ($values as $value) {
                $val = implode(', ', array_map([$this, 'escape'], $value));
                $query .= '(' . $val . '), ';
            }
            $query = trim($query, ', ');
        } else {
            $column = implode(', ', array_keys($data));
            $val = implode(', ', array_map([$this, 'escape'], $data));
            $query .= ' (' . $column . ') VALUES (' . $val . ')';
        }

        if ($type === true)
            return $query;

        if ($this->query($query, false)) {
            $this->insertId = $this->pdo->lastInsertId();
            return $this->insertId();
        }

        return false;
    }

    /**
     * @param array $data
     * @param bool  $type
     *
     * @return mixed|string
     */
    public function update(array $data, $type = false)
    {
        $query = 'UPDATE ' . $this->from . ' SET ';
        $values = [];

        foreach ($data as $column => $val)
            $values[] = $column . '=' . $this->escape($val);

        $query .= implode(',', $values);

        if (!is_null($this->where))
            $query .= ' WHERE ' . $this->where;

        if (!is_null($this->orderBy))
            $query .= ' ORDER BY ' . $this->orderBy;

        if (!is_null($this->limit))
            $query .= ' LIMIT ' . $this->limit;

        return $type === true ? $query : $this->query($query, false);
    }

    /**
     * @param bool $type
     *
     * @return mixed|string
     */
    public function delete($type = false)
    {
        $query = 'DELETE FROM ' . $this->from;

        if (!is_null($this->where))
            $query .= ' WHERE ' . $this->where;

        if (!is_null($this->orderBy))
            $query .= ' ORDER BY ' . $this->orderBy;

        if (!is_null($this->limit))
            $query .= ' LIMIT ' . $this->limit;

        if ($query === 'DELETE FROM ' . $this->from)
            $query = 'TRUNCATE TABLE ' . $this->from;

        return $type === true ? $query : $this->query($query, false);
    }

    /**
     * @return mixed
     */
    public function analyze()
    {
        return $this->query('ANALYZE TABLE ' . $this->from, false);
    }

    /**
     * @return mixed
     */
    public function check()
    {
        return $this->query('CHECK TABLE ' . $this->from, false);
    }

    /**
     * @return mixed
     */
    public function checksum()
    {
        return $this->query('CHECKSUM TABLE ' . $this->from, false);
    }

    /**
     * @return mixed
     */
    public function optimize()
    {
        return $this->query('OPTIMIZE TABLE ' . $this->from, false);
    }

    /**
     * @return mixed
     */
    public function repair()
    {
        return $this->query('REPAIR TABLE ' . $this->from, false);
    }

    /**
     * @return bool
     */
    public function transaction(): bool
    {
        if (!$this->transactionCount++)
            return $this->pdo->beginTransaction();

        $this->pdo->exec('SAVEPOINT trans' . $this->transactionCount);
        return $this->transactionCount >= 0;
    }

    /**
     * @return bool
     */
    public function commit(): bool
    {
        if (!--$this->transactionCount)
            return $this->pdo->commit();

        return $this->transactionCount >= 0;
    }

    /**
     * @return bool
     */
    public function rollBack(): bool
    {
        if (--$this->transactionCount) {
            $this->pdo->exec('ROLLBACK TO trans' . ($this->transactionCount + 1));
            return true;
        }

        return $this->pdo->rollBack();
    }

    /**
     * @return mixed
     */
    public function exec()
    {
        if (is_null($this->query))
            return null;

        $query = $this->pdo->exec($this->query);
        if ($query === false) {
            $this->error = $this->pdo->errorInfo()[2];
            $this->error();
        }

        return $query;
    }

    /**
     * @param string|null $type
     * @param string|null $argument
     * @param bool $all
     *
     * @return mixed
     */
    public function fetch(string $type = null, string $argument = null, bool $all = false)
    {
        if (is_null($this->query)) {
            return null;
        }

        $query = $this->pdo->query($this->query);
        if (!$query) {
            $this->error = $this->pdo->errorInfo()[2];
            $this->error();
        }

        $type = $this->getFetchType($type);
        if ($type === PDO::FETCH_CLASS)
            $query->setFetchMode($type, $argument);
        else
            $query->setFetchMode($type);

        $result = $all ? $query->fetchAll() : $query->fetch();
        $this->numRows = is_array($result) ? count($result) : 1;
        return $result;
    }

    /**
     * @param string|null $type
     * @param string|null $argument
     *
     * @return mixed
     */
    public function fetchAll(string $type = null, string $argument = null)
    {
        return $this->fetch($type, $argument, true);
    }

    /**
     * @param string $query
     * @param array|bool $all
     * @param string|null $type
     * @param string|null $argument
     *
     * @return $this|mixed
     */
    public function query(string $query, $all = true, string $type = null, string $argument = null)
    {
        $this->reset();

        if (is_array($all) || func_num_args() === 1) {
            $params = explode('?', $query);
            $newQuery = '';
            foreach ($params as $key => $value) {
                if (!empty($value))
                    $newQuery .= $value . (isset($all[$key]) ? $this->escape($all[$key]) : '');
            }
            $this->query = $newQuery;
            return $this;
        }

        $this->query = preg_replace('/\s\s+|\t\t+/', ' ', trim($query));
        $str = false;
        foreach (['select', 'optimize', 'check', 'repair', 'checksum', 'analyze'] as $value) {
            if (stripos($this->query, $value) === 0) {
                $str = true;
                break;
            }
        }

        $type = $this->getFetchType($type);
        $cache = false;
        if (!is_null($this->cache) && $type !== PDO::FETCH_CLASS)
            $cache = $this->cache->getCache($this->query, $type === PDO::FETCH_ASSOC);

        if (!$cache && $str) {
            $sql = $this->pdo->query($this->query);
            if ($sql) {
                $this->numRows = $sql->rowCount();
                if ($this->numRows > 0) {
                    if ($type === PDO::FETCH_CLASS)
                        $sql->setFetchMode($type, $argument);
                    else
                        $sql->setFetchMode($type);
                    $this->result = $all ? $sql->fetchAll() : $sql->fetch();
                }

                if (!is_null($this->cache) && $type !== PDO::FETCH_CLASS)
                    $this->cache->setCache($this->query, $this->result);
                $this->cache = null;
            } else {
                $this->cache = null;
                $this->error = $this->pdo->errorInfo()[2];
                $this->error();
            }
        } elseif ((!$cache && !$str) || ($cache && !$str)) {
            $this->cache = null;
            $this->result = $this->pdo->exec($this->query);

            if ($this->result === false) {
                $this->error = $this->pdo->errorInfo()[2];
                $this->error();
            }
        } else {
            $this->cache = null;
            $this->result = $cache;
            $this->numRows = is_array($this->result) ? count($this->result) : ($this->result === '' ? 0 : 1);
        }

        $this->queryCount++;
        return $this->result;
    }

    /**
     * @param $data
     *
     * @return string
     */
    public function escape($data)
    {
        return $data === null ? 'NULL' : (
        is_int($data) || is_float($data) ? $data : $this->pdo->quote($data)
        );
    }

    /**
     * @param $time
     *
     * @return $this
     */
    public function cache($time): DB
    {
        $this->cache = new Cache($this->cacheDir, $time);

        return $this;
    }

    /**
     * @return int
     */
    public function queryCount(): int
    {
        return $this->queryCount;
    }

    /**
     * @return string|null
     */
    public function getQuery(): ?string
    {
        return $this->query;
    }

    /**
     * @return void
     */
    public function __destruct()
    {
        $this->pdo = null;
    }

    /**
     * @return void
     */
    protected function reset()
    {
        $this->select = '*';
        $this->from = null;
        $this->where = null;
        $this->limit = null;
        $this->offset = null;
        $this->orderBy = null;
        $this->groupBy = null;
        $this->having = null;
        $this->join = null;
        $this->grouped = false;
        $this->numRows = 0;
        $this->insertId = null;
        $this->query = null;
        $this->error = null;
        $this->result = [];
        $this->transactionCount = 0;
    }

    /**
     * @param  $type
     *
     * @return int
     */
    protected function getFetchType($type): int
    {
        return $type === 'class'
            ? PDO::FETCH_CLASS
            : ($type === 'array'
                ? PDO::FETCH_ASSOC
                : PDO::FETCH_OBJ);
    }

    /**
     * @param string $fields
     *
     * @return void
     */
    private function optimizeSelect(string $fields)
    {
        $this->select = $this->select === '*'
            ? $fields
            : $this->select . ', ' . $fields;
    }
}
