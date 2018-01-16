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
    private $aggs;
    private $_pendingParams = [];
    private $_nullDsl = ['term' => ['_id' => '-1']];
    private $_rangeParams = [
        'gt'  => '>',
        'lt'  => '<',
        'gte' => '>=',
        'lte' => '<=',
        'no'  => '><'
    ];
    private $_replaceArray = [
        '\\' => '\\\\',
        '+'  => '\\+',
        '-'  => '\\-',
        '&&' => '\\&&',
        '||' => '\\||',
        '!'  => '\\!',
        '('  => '\\(',
        ')'  => '\\)',
        '{'  => '\\{',
        '}'  => '\\}',
        '['  => '\\[',
        ']'  => '\\]',
        '^'  => '\\^',
        '”'  => '\\”',
        '~'  => '\\~',
        '*'  => '\\*',
        '?'  => '\\?',
        ':'  => '\\:',
        '/'  => '\\/',
        ' '  => '\\ '
    ]; // 转义字符
    private $_metricsArray = [
        'filter',
        'avg',
        'cardinality',
        'extended_stats',
        'geo_bounds',
        'geo_centroid',
        'max',
        'min',
        'percentiles',
        'percentile_ranks',
        'scripted_metric',
        'stats',
        'sum',
        'value_count',
        'top_hits',
        'terms' // 普通聚合
    ];

    /**
     * @return mixed
     * @throws Exception
     */
    public function build()
    {
        $params = [];
        $this->dsl['query'] = $this->buildWhere($this->where, $params);
        $this->select && $this->dsl['_source'] = $this->buildSelect($this->select);
        $this->orderBy && $this->dsl['sort'] = $this->buildOrderBy($this->orderBy);
        $this->groupBy && $this->dsl['aggs'] = $this->buildGroupBy($this->groupBy);
        $this->aggs && $this->dsl['aggs'] = $this->buildAggs($this->aggs);
        $this->limit !== null && $this->dsl['size'] = $this->limit;
        $this->offset && $this->dsl['from'] = $this->offset;
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

    /**
     * @param $condition
     * @param $params
     *
     * @return array
     * @throws Exception
     */
    private function buildWhere($condition, &$params)
    {
        if ($this->debug) {
            p($condition);
        }
        $where = $this->buildCondition($condition, $params);
        return $where ? ['bool' => $where] : ['match_all' => []];
    }

    /**
     * @param $condition
     * @param $params
     *
     * @return array
     * @throws Exception
     */
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
                default :
                    return $this->buildSimpleCondition($operator, $condition, $params);
            }
        } else {
            return $this->buildHashCondition($condition, $params);
        }
    }

    /**
     * @param $condition
     * @param $params
     *
     * @return array
     * @throws Exception
     */
    private function buildHashCondition($condition, &$params)
    {
        $parts = [];
        foreach ($condition as $column => $value) {
            if (is_array($value)) {
                $inCondition = $this->buildInCondition('IN', [$column, $value], $params);
                isset($inCondition['must']) && $parts['must'][] = $inCondition['must'];
                isset($inCondition['must_not']) && $parts['must_not'][] = $inCondition['must_not'];
            } else {
                if ($value === null) {
                    $parts['must_not'][] = ['exists' => ['field' => $column]];
                } elseif ($value === true) {
                    $parts['must'][] = ['exists' => ['field' => $column]];
                } else {
                    $phName = self::PARAM_PREFIX . count($params);
                    $parts['must'][] = ['term' => [$column => $phName]];
                    $params[$phName] = $value;
                }
            }
        }
        return $parts ? $parts : [];
    }

    /**
     * @param $operator
     * @param $operands
     * @param $params
     *
     * @return array
     * @throws Exception
     */
    private function buildAndCondition($operator, $operands, &$params)
    {    // 待优化，如何合并同级
        $parts = [];
        $operator = ($operator == 'AND') ? 'must' : 'should';
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

    /**
     * @param $operator
     * @param $operands
     * @param $params
     *
     * @return array|string
     * @throws Exception
     */
    private function buildNotCondition($operator, $operands, &$params)
    {
        if (count($operands) !== 1) {
            throw new Exception("Operator '$operator' requires exactly one operand.");
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

    /**
     * @param $operator
     * @param $operands
     * @param $params
     *
     * @return array
     * @throws Exception
     */
    private function buildBetweenCondition($operator, $operands, &$params)
    {
        if (!isset($operands[0], $operands[1], $operands[2])) {
            throw new Exception("Operator '$operator' requires three operands.");
        }
        list($column, $value1, $value2) = $operands;
        $phName1 = self::PARAM_PREFIX . count($params);
        $params[$phName1] = $value1;
        $phName2 = self::PARAM_PREFIX . count($params);
        $params[$phName2] = $value2;
        $operator = ($operator == 'BETWEEN') ? 'must' : 'must_not';
        return [$operator => ['range' => [$column => ['gte' => $phName1, 'lte' => $phName2]]]];
    }

    /**
     * @param $operator
     * @param $operands
     * @param $params
     *
     * @return array
     * @throws Exception
     */
    private function buildLikeCondition($operator, $operands, &$params)
    {
        if (!isset($operands[0], $operands[1])) {
            throw new Exception("Operator '$operator' requires two operands.");
        }
        if (!preg_match('/^(AND |OR |)(((NOT |))I?LIKE)/', $operator, $matches)) {
            throw new Exception("Invalid operator '$operator'.");
        }
        $andor = (!empty($matches[1]) ? $matches[1] : 'AND');
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
            return ($operator == 'NOT LIKE') ? ['should' => ['bool' => ['must_not' => $parts]]] : ['should' => $parts];
        } else {
            $phName = self::PARAM_PREFIX . count($params);
            $params[$phName] = $values[0];
            $operator = ($operator == 'NOT LIKE') ? 'must_not' : 'must';
            return [$operator => ['wildcard' => [$column => $phName]]];
        }
    }

    /**
     * @param $operator
     * @param $operands
     * @param $params
     *
     * @return array
     * @throws Exception
     */
    private function buildInCondition($operator, $operands, &$params)
    {
        if (!isset($operands[0], $operands[1])) {
            throw new Exception("Operator '$operator' requires two operands.");
        }
        list($column, $values) = $operands;
        if ($values === [] || $column === []) {
            return $operator === 'IN' ? $this->_nullDsl : [];
        }
        $values = (array)$values;
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

    /**
     * @param $operator
     * @param $operands
     * @param $params
     *
     * @return array
     * @throws Exception
     */
    private function buildSimpleCondition($operator, $operands, &$params)
    {
        if (count($operands) !== 2) {
            throw new Exception("Operator '$operator' requires two operands.");
        }
        if (!in_array($operator, $this->_rangeParams)) {
            throw new Exception("Operator '$operator' not in [ '>', '>=', '<', '<=', '><' ]");
        }
        list($column, $value) = $operands;
        $operator = array_search($operator, $this->_rangeParams);
        if ($operator === 'no') {
            return ['must_not' => ['term' => [$column => $value]]];
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
            $type = ($type == 'asc') ? 'asc' : 'desc';
            $sort[$key] = ['order' => $type];
        }
        return $sort;
    }

    /**
     *
     * ['block','citycode']
     * ['block'=>['offset'=>1,'limit'=>2,'field'=>['district','block']]]
     * ['block'=>['offset'=>1,'limit'=>2,'field'=>['district','block']],'citycode']
     *
     * @param array $columns
     *
     * @return array
     */
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
                $group['group_by_' . $key] = [
                    'terms' => ['field' => $key],
                    'aggs'  => [
                        $key => [
                            'top_hits' => [
                                'from'    => isset($value['offset']) ? intval($value['offset']) : 0,
                                'size'    => isset($value['limit']) ? intval($value['limit']) : 0,
                                '_source' => isset($value['field']) ? $value['field'] : [],
                            ]
                        ]
                    ]
                ];
            } else {
                $group['group_by_' . $value] = ['terms' => ['field' => $value]];
            }
        }
        return $group;
    }

    /**
     * ['agentid' => ['terms' => ['field' => 'agentid'],
     * 'aggs'  => ['tradetype' => ['terms' => ['field' => 'tradetype'],
     * 'aggs'  => ['room' => ['terms' => ['field' => 'room'],
     * 'aggs'  => ['min_price' => ['min' => 'price']
     * ]]]]]]]
     *
     * @param array $columns
     *
     * @return array
     */
    private function buildAggs($columns)
    {
        if (empty($columns)) {
            return [];
        }
        if ($this->debug) {
            p($columns);
        }
        $aggs = [];
        foreach ($columns as $key => $value) {
            if (array_intersect($this->_metricsArray, array_keys($value))) {
                if (isset($value['terms'])) {
                    $aggs[$key]['terms']['field'] = $value['terms']['field'];
                    isset($value['terms']['order']) && $aggs[$key]['terms']['order'] = $value['terms']['order'];
                    isset($value['terms']['size']) && $aggs[$key]['terms']['size'] = $value['terms']['size'];
                }
                isset($value['avg']) && $aggs[$key]['avg']['field'] = $value['avg'];
                isset($value['min']) && $aggs[$key]['min']['field'] = $value['min'];
                isset($value['max']) && $aggs[$key]['max']['field'] = $value['max'];
                isset($value['cardinality']) && $aggs[$key]['cardinality'] = $value['cardinality'];
                isset($value['filter']) && $aggs[$key]['filter'] = $value['filter'];
                if (isset($value['top_hits'])) {
                    isset($value['top_hits']['sort']) && $aggs[$key]['top_hits']['sort'] = $value['top_hits']['sort'];
                    isset($value['top_hits']['size']) && $aggs[$key]['top_hits']['size'] = $value['top_hits']['size'];
                    isset($value['top_hits']['_source']) && $aggs[$key]['top_hits']['_source'] = $value['top_hits']['_source'];
                }
                // add other ... [$this->_metricsArray]

                if (isset($value['aggs'])) {
                    $aggs[$key]['aggs'] = $this->buildAggs($value['aggs']);
                }
            }
        }
        return $aggs;
    }

    public function select($columns)
    {
        if (!is_array($columns)) {
            $columns = preg_split('/\s*,\s*/', trim($columns), -1, PREG_SPLIT_NO_EMPTY);
        }
        $this->select = $columns;
        return $this;
    }

    public function addSelect($columns)
    {
        if (!is_array($columns)) {
            $columns = preg_split('/\s*,\s*/', trim($columns), -1, PREG_SPLIT_NO_EMPTY);
        }
        if ($this->select === null) {
            $this->select = $columns;
        } else {
            $this->select = array_merge($this->select, $columns);
        }
        return $this;
    }

    /**
     * 可以使用两种方式：
     *  哈希格式，例如： ['column1' => value1, 'column2' => value2, ...]
     *  操作符格式，例如：[operator,operand1, operand2, ...]
     *
     * 哈希格式
     *   ['type' => 1, 'status' => 2]
     *   ['id' => [1, 2, 3], 'status' => 2]
     *   ['status' => null]
     *
     * 操作符格式
     *   [操作符, 操作数1, 操作数2, ...]
     *   ['and', ['district' => 1337], ['block' => 1337]]
     *   ['and', ['district' => 1337], ['or', ['block' => 1338], ['block' => 1339]]]
     *   ['or', ['type' => [7, 8, 9]], ['id' => [1, 2, 3]]
     *   ['not', ['attribute' => null]]
     *   ['between', 'id', 1, 10]
     *   ['in', 'id', [1, 2, 3]]
     *   ['in', ['id', 'name'], [['id' => 1, 'name' => 'foo'], ['id' => 2, 'name' => 'bar']] ]
     *   ['like', 'name', 'tester']
     *   ['like', 'name', ['test', 'sample']]
     *   ['>=', 'id', 10]
     *
     * @param array $condition
     * @param array $params
     *
     * @return Elasticsearch_Dsl
     */
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
            $columns = preg_split('/\s*,\s*/', trim($columns), -1, PREG_SPLIT_NO_EMPTY);
        }
        $this->groupBy = $columns;
        return $this;
    }

    public function aggs($columns)
    {
        if (!is_array($columns)) {
            $columns = preg_split('/\s*,\s*/', trim($columns), -1, PREG_SPLIT_NO_EMPTY);
        }
        $this->aggs = $columns;
        return $this;
    }

    public function orderBy($columns)
    {
        if (!is_array($columns)) {
            $columns = preg_split('/\s*,\s*/', trim($columns), -1, PREG_SPLIT_NO_EMPTY);
            $result = [];
            foreach ($columns as $column) {
                if (preg_match('/^(.*?)\s+(asc|desc)$/i', $column, $matches)) {
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
        $this->_pendingParams = array_reverse($this->_pendingParams); // 要重大到小替换，避免出错
        $this->dsl = str_replace(array_keys($this->_pendingParams), array_values($this->_pendingParams), json_encode($this->dsl));
        $this->_pendingParams = [];
    }

    public function bindValues($values)
    {
        if (empty($values)) {
            return $this;
        }
        foreach ($values as $name => $value) {
            $this->_pendingParams[$name] = str_replace(array_keys($this->_replaceArray), array_values($this->_replaceArray), $value);
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
