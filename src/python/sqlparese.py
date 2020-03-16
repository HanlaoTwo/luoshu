import sqlparse
from sqlparse.sql import Where, Comparison, Parenthesis
from sqlparse import sql
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML, DDL

import neo_helper

ALL_JOIN_TYPE = ('LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'FULL JOIN', 'LEFT OUTER JOIN', 'FULL OUTER JOIN')


def get_table_name(query_tokens):
    echo = False
    for token in query_tokens:
        if echo and not token.match(None, 'select.*from.*', True):
            if len(token.value.strip()) == 0:
                continue
            echo = False
            if token.ttype is None and isinstance(token, Identifier):
                db = token.get_parent_name()
                table = token.get_real_name()
                if db == None:
                    # print('----',table)
                    yield table
                else:
                    res = '{0}.{1}'.format(token.get_parent_name(), token.get_real_name())
                    # print('----',res)
                    yield res

        if token.ttype is None and isinstance(token, sql.IdentifierList):
            # print('-----------------------------------')
            # print('Identifierlist:')
            # for id in token.get_identifiers():
            #     print(id)
            pass

        if (token.ttype is Keyword and token.value.upper() == 'FROM') or token.value.upper() in ALL_JOIN_TYPE:
            echo = True
            continue

        if token.match(None, 'select.*from.*', True):
            for sub_token in token.get_sublists():
                subsql = sub_token.value.replace('(', '').replace(')', '')
                sub_tokens = sqlparse.parse(subsql)[0]
                t = get_table_name(sub_tokens)
                for i in t :
                    yield i


def get_create(query_tokens):
    echo = False
    for token in query_tokens:
        if echo:
            if len(token.value.strip()) == 0 or token.value.upper() == 'TABLE':
                continue
            echo = False
            if token.ttype is None and isinstance(token, Identifier):
                # print('token[%s] type[%s]' % (token, token.ttype))
                # 库名。表名
                db = token.get_parent_name()
                table = token.get_real_name()
                if db == None:
                    return table
                else:
                    return '{0}.{1}'.format(token.get_parent_name(), token.get_real_name())

        if token.ttype is DDL and token.value.upper() == 'CREATE':
            echo = True
            continue


with open('sample.sql', 'r', encoding='utf8') as sqlstr:
    for parseed in sqlparse.parse(sqlstr):
        query_tokens = parseed.tokens
        son = get_create(query_tokens)
        parents = list(get_table_name(query_tokens))
        print(son)
        print(parents)
        neo_helper.save_table_from(son,parents)

