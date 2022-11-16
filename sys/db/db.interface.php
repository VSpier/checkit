<?php

namespace lib;

interface DbInterface
{
    /**
     * @param null $type
     * @param null $argument
     *
     * @return mixed
     */
    public function get($type = null, $argument = null);

    /**
     * @param null $type
     * @param null $argument
     *
     * @return mixed
     */
    public function getAll($type = null, $argument = null);

    /**
     * @param array $data
     * @param bool $type
     *
     * @return mixed
     */
    public function update(array $data, bool $type = false);

    /**
     * @param array $data
     * @param bool  $type
     *
     * @return mixed
     */
    public function insert(array $data, bool $type = false);

    /**
     * @param bool $type
     *
     * @return mixed
     */
    public function delete(bool $type = false);
}
