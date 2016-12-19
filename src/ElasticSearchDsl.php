<?php
namespace Xiaobeicn\ElasticSearchDsl;

class ElasticSearchDsl
{
    const PARAM_PREFIX = ':qp';
    private $debug = false;
    private $dsl;
    private $where;
    private $limit;
    private $offset;
    private $orderBy;
    private $select;
    private $groupBy;
    private $_pendingParams = [];
    private $_nullDsl = ['term' => ['_id' => '-1']];
    private $_rangeParams = ['gt' => '>', 'lt' => '<', 'gte' => '>=', 'lte' => '<=', 'no' => '><'];
    private $replaceArray = array('\\' => '\\\\', '+' => '\\+', '-' => '\\-', '&&' => '\\&&', '||' => '\\||', '!' => '\\!', '(' => '\\(', ')' => '\\)', '{' => '\\{', '}' => '\\}', '[' => '\\[', ']' => '\\]', '^' => '\\^', '”' => '\\”', '~' => '\\~', '*' => '\\*', '?' => '\\?', ':' => '\\:', '/' => '\\/', ' ' => '\\ ');

    public function build()
    {
        $params = [];
        $this->dsl['query'] = $this->buildWhere($this->where, $params);
        $this->select && ($this->dsl['_source'] = $this->buildSelect($this->select));
        $this->orderBy && ($this->dsl['sort'] = $this->buildOrderBy($this->orderBy));
        $this->groupBy && ($this->dsl['aggs'] = $this->buildGroupBy($this->groupBy));
        $this->limit && ($this->dsl['size'] = $this->limit);
        $this->offset && ($this->dsl['from'] = $this->offset);
        $this->bindValues($params);
        $this->bindPendingParams();
        return $this->dsl;
    }

    private function buildSelect($columns)
    {
        if (empty($columns)) {
            return [];
        }
        return $columns;
    }

    private function buildWhere($condition, &$params)
    {
        if ($this->debug) {
            p($condition);
        }
        $where = $this->buildCondition($condition, $params);
        return $where ? ['bool' => $where] : ['match_all' => []];
    }

    private function buildCondition($condition, &$params)
    {
        if (!is_array($condition) || empty($condition)) {
            return [];
        }
        if (isset($condition[0])) {
            $operator = strtoupper($condition[0]);
            array_shift($condition);
            switch ($operator) {
                case 'NOT':
                    return $this->buildNotCondition($operator, $condition, $params);
                case 'AND':
                case 'OR':
                    return $this->buildAndCondition($operator, $condition, $params);
                case 'BETWEEN':
                case 'NOT BETWEEN':
                    return $this->buildBetweenCondition($operator, $condition, $params);
                case 'IN':
                case 'NOT IN':
                    return $this->buildInCondition($operator, $condition, $params);
                case 'LIKE':
                case 'NOT LIKE':
                case 'OR LIKE':
                case 'OR NOT LIKE':
                    return $this->buildLikeCondition($operator, $condition, $params);
                default:
                    return $this->buildSimpleCondition($operator, $condition, $params);
            }
        } else {
            return $this->buildHashCondition($condition, $params);
        }
    }

    private function buildHashCondition($condition, &$params)
    {
        $parts = [];
        foreach ($condition as $column => $value) {
            if (is_array($value)) {
                $inCondition = $this->buildInCondition('IN', [$column, $value], $params);
                isset($inCondition['must']) && ($parts['must'][] = $inCondition['must']);
                isset($inCondition['must_not']) && ($parts['must_not'][] = $inCondition['must_not']);
            } else {
                if ($value === null) {
                    $parts['must_not'][] = ['exists' => ['field' => $column]];
                } else {
                    $phName = self::PARAM_PREFIX . count($params);
                    $parts['must'][] = ['term' => [$column => $phName]];
                    $params[$phName] = $value;
                }
            }
        }
        return $parts ? $parts : [];
    }

    private function buildAndCondition($operator, $operands, &$params)
    {
        // 待优化，如何合并同级
        $parts = [];
        $operator = $operator == 'AND' ? 'must' : 'should';
        foreach ($operands as $operand) {
            if (is_array($operand)) {
                $operand = $this->buildCondition($operand, $params);
                if (isset($operand['must'][0]) && count($operand['must']) == 1) {
                    $parts[$operator][] = $operand['must'][0];
                } elseif ($operand) {
                    $parts[$operator][] = ['bool' => $operand];
                }
            }
        }
        return $parts;
    }

    private function buildNotCondition($operator, $operands, &$params)
    {
        if (count($operands) !== 1) {
            throw new \Exception("Operator '{$operator}' requires exactly one operand.");
        }
        $operand = reset($operands);
        if (is_array($operand)) {
            $operand = $this->buildCondition($operand, $params);
        }
        if ($operand === '') {
            return '';
        }
        return ['must_not' => ['bool' => $operand]];
    }

    private function buildBetweenCondition($operator, $operands, &$params)
    {
        if (!isset($operands[0], $operands[1], $operands[2])) {
            throw new \Exception("Operator '{$operator}' requires three operands.");
        }
        list($column, $value1, $value2) = $operands;
        $phName1 = self::PARAM_PREFIX . count($params);
        $params[$phName1] = $value1;
        $phName2 = self::PARAM_PREFIX . count($params);
        $params[$phName2] = $value2;
        $operator = $operator == 'BETWEEN' ? 'must' : 'must_not';
        return [$operator => ['range' => [$column => ['gte' => $phName1, 'lte' => $phName2]]]];
    }

    private function buildLikeCondition($operator, $operands, &$params)
    {
        if (!isset($operands[0], $operands[1])) {
            throw new \Exception("Operator '{$operator}' requires two operands.");
        }
        if (!preg_match('/^(AND |OR |)(((NOT |))I?LIKE)/', $operator, $matches)) {
            throw new \Exception("Invalid operator '{$operator}'.");
        }
        $andor = !empty($matches[1]) ? $matches[1] : 'AND';
        $not = !empty($matches[3]);
        $operator = $matches[2];
        list($column, $values) = $operands;
        if (!is_array($values)) {
            $values = [$values];
        }
        if (empty($values)) {
            return $not ? [] : $this->_nullDsl;
        }
        $parts = [];
        if (trim($andor) == 'OR') {
            foreach ($values as $value) {
                $phName = self::PARAM_PREFIX . count($params);
                $params[$phName] = $value;
                $parts[] = ['wildcard' => [$column => $phName]];
            }
            return $operator == 'NOT LIKE' ? ['should' => ['bool' => ['must_not' => $parts]]] : ['should' => $parts];
        } else {
            $phName = self::PARAM_PREFIX . count($params);
            $params[$phName] = $values[0];
            $operator = $operator == 'NOT LIKE' ? 'must_not' : 'must';
            return [$operator => ['wildcard' => [$column => $phName]]];
        }
    }

    private function buildInCondition($operator, $operands, &$params)
    {
        if (!isset($operands[0], $operands[1])) {
            throw new \Exception("Operator '{$operator}' requires two operands.");
        }
        list($column, $values) = $operands;
        if ($values === [] || $column === []) {
            return $operator === 'IN' ? $this->_nullDsl : [];
        }
        $values = (array) $values;
        if (count($column) > 1) {
            return $this->buildCompositeInCondition($operator, $column, $values, $params);
        }
        if (is_array($column)) {
            $column = reset($column);
        }
        foreach ($values as $i => $value) {
            if (is_array($value)) {
                $value = isset($value[$column]) ? $value[$column] : null;
            }
            if ($value === null) {
                unset($values[$i]);
            } else {
                $phName = self::PARAM_PREFIX . count($params);
                $params[$phName] = $value;
                $values[$i] = $phName;
            }
        }
        $operator = $operator === 'IN' ? 'must' : 'must_not';
        if (count($values) > 1) {
            return [$operator => ['terms' => [$column => array_values($values)]]];
        } else {
            return [$operator => ['term' => [$column => reset($values)]]];
        }
    }

    private function buildCompositeInCondition($operator, $columns, $values, &$params)
    {
        $vss = [];
        foreach ($values as $value) {
            foreach ($columns as $column) {
                if (isset($value[$column])) {
                    $phName = self::PARAM_PREFIX . count($params);
                    $params[$phName] = $value[$column];
                    $vss[$column][] = $phName;
                }
            }
        }
        $parts = [];
        $operator = $operator === 'IN' ? 'must' : 'must_not';
        foreach ($columns as $c) {
            $parts[$operator][] = ['terms' => [$c => $vss[$c]]];
        }
        return $parts;
    }

    private function buildSimpleCondition($operator, $operands, &$params)
    {
        if (count($operands) !== 2) {
            throw new \Exception("Operator '{$operator}' requires two operands.");
        }
        if (!in_array($operator, $this->_rangeParams)) {
            throw new \Exception("Operator '{$operator}' not in [ '>', '>=', '<', '<=', '><' ]");
        }
        list($column, $value) = $operands;
        $operator = array_search($operator, $this->_rangeParams);
        if ($operator === 'no') {
            return ['must_not' => ['match' => ['term' => [$column => $value]]]];
        } else {
            $phName = self::PARAM_PREFIX . count($params);
            $params[$phName] = $value;
            return ['must' => ['range' => [$column => [$operator => $value]]]];
        }
    }

    private function buildOrderBy($columns)
    {
        if (empty($columns)) {
            return [];
        }
        $sort = [];
        foreach ($columns as $key => $type) {
            $type = strtolower($type);
            $type = $type == 'asc' ? 'asc' : 'desc';
            $sort[$key] = ['order' => $type];
        }
        return $sort;
    }

    private function buildGroupBy($columns)
    {
        if (empty($columns)) {
            return [];
        }
        if ($this->debug) {
            p($columns);
        }
        $group = [];
        foreach ($columns as $key => $value) {
            if (is_array($value)) {
                $group['group_by_' . $key] = ['terms' => ['field' => $key], 'aggs' => [$key => ['top_hits' => ['from' => isset($value['offset']) ? intval($value['offset']) : 0, 'size' => isset($value['limit']) ? intval($value['limit']) : 0, '_source' => isset($value['field']) ? $value['field'] : []]]]];
            } else {
                $group['group_by_' . $value] = ['terms' => ['field' => $value]];
            }
        }
        return $group;
    }

    public function select($columns)
    {
        if (!is_array($columns)) {
            $columns = preg_split('/\\s*,\\s*/', trim($columns), -1, PREG_SPLIT_NO_EMPTY);
        }
        $this->select = $columns;
        return $this;
    }

    public function addSelect($columns)
    {
        if (!is_array($columns)) {
            $columns = preg_split('/\\s*,\\s*/', trim($columns), -1, PREG_SPLIT_NO_EMPTY);
        }
        if ($this->select === null) {
            $this->select = $columns;
        } else {
            $this->select = array_merge($this->select, $columns);
        }
        return $this;
    }
    
    public function where($condition, $params = [])
    {
        $this->where = $condition;
        return $this->bindValues($params);
    }

    public function andWhere($condition, $params = [])
    {
        if ($this->where === null) {
            $this->where = $condition;
        } else {
            $this->where = ['and', $this->where, $condition];
        }
        return $this->bindValues($params);
    }

    public function orWhere($condition, $params = [])
    {
        if ($this->where === null) {
            $this->where = $condition;
        } else {
            $this->where = ['or', $this->where, $condition];
        }
        return $this->bindValues($params);
    }

    public function groupBy($columns)
    {
        if (!is_array($columns)) {
            $columns = preg_split('/\\s*,\\s*/', trim($columns), -1, PREG_SPLIT_NO_EMPTY);
        }
        $this->groupBy = $columns;
        return $this;
    }

    public function orderBy($columns)
    {
        if (!is_array($columns)) {
            $columns = preg_split('/\\s*,\\s*/', trim($columns), -1, PREG_SPLIT_NO_EMPTY);
            $result = [];
            foreach ($columns as $column) {
                if (preg_match('/^(.*?)\\s+(asc|desc)$/i', $column, $matches)) {
                    $result[$matches[1]] = strcasecmp($matches[2], 'desc') ? 'ASC' : 'DESC';
                } else {
                    $result[$column] = 'ASC';
                }
            }
            $this->orderBy = $result;
        } else {
            $this->orderBy = $columns;
        }
        return $this;
    }

    public function limit($limit)
    {
        $this->limit = $limit;
        return $this;
    }

    public function offset($offset)
    {
        $this->offset = $offset;
        return $this;
    }

    public function page($page, $pagesize)
    {
        $this->offset = ($page - 1) * $pagesize;
        $this->limit = $pagesize;
        return $this;
    }

    private function bindPendingParams()
    {
        if ($this->debug) {
            p($this->_pendingParams);
        }
        $this->dsl = str_replace(array_keys($this->_pendingParams), array_values($this->_pendingParams), json_encode($this->dsl));
        $this->_pendingParams = [];
    }

    public function bindValues($values)
    {
        if (empty($values)) {
            return $this;
        }
        foreach ($values as $name => $value) {
            $this->_pendingParams[$name] = str_replace(array_keys($this->replaceArray), array_values($this->replaceArray), $value);
        }
        return $this;
    }

    public function reSet()
    {
        $this->select = null;
        $this->where = null;
        $this->limit = null;
        $this->offset = null;
        $this->orderBy = null;
        $this->groupBy = null;
        $this->_pendingParams = [];
        return $this;
    }

    public function debug()
    {
        $this->debug = true;
        return $this;
    }

}