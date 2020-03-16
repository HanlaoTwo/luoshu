import sqlparse
from sqlparse.sql import Where, Comparison, Parenthesis
from sqlparse import sql
from sqlparse.sql import IdentifierList, Identifier

from sqlparse.tokens import Keyword, DML

# 支持的join方式
ALL_JOIN_TYPE = ('LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'FULL JOIN', 'LEFT OUTER JOIN', 'FULL OUTER JOIN')


def is_subselect(parsed):
    """
    是否子查询
    :param parsed:
    :return:
    """
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False


def extract_from_part(parsed):
    """
    提取from之后模块
    :param parsed:
    :return:
    """
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
                    yield x
            elif item.ttype is Keyword:
                from_seen = False
                continue
            else:
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True


def extract_join_part(parsed):
    """
    提交join之后模块
    :param parsed:
    :return:
    """
    flag = False
    for item in parsed.tokens:
        if flag:
            if item.ttype is Keyword:
                flag = False
                continue
            else:
                yield item
        if item.ttype is Keyword and item.value.upper() in ALL_JOIN_TYPE:
            print('join-------')
            flag = True


def extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier.get_name()
        elif isinstance(item, Identifier):
            yield item.get_real_name(),item.get_parent_name()
        elif item.ttype is Keyword:
            yield item.value


def extract_tables(sql):
    """
    提取sql中的表名（select语句）
    :param sql:
    :return:
    """
    from_stream = extract_from_part(sqlparse.parse(sql)[0])
    join_stream = extract_join_part(sqlparse.parse(sql)[0])
    return list(extract_table_identifiers(from_stream)) + list(extract_table_identifiers(join_stream))

with open('sample.sql', 'r', encoding='utf8') as sql:
    # print(sql.read())
    print(extract_tables(sql.read()))
