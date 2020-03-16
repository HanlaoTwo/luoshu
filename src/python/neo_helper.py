from py2neo import Node, Relationship,Graph,NodeMatcher,RelationshipMatcher


graph =  Graph("http://127.0.0.1:7474",username="neo4j",password="000000")
node_matcher = NodeMatcher(graph)
relation_matcher = RelationshipMatcher(graph)

def save_table_from(table,from_table):
    table = Node('table',name=table)
    graph.create(table)
    for parent in from_table:
        parent_t = Node('table',name=parent)
        r = Relationship(table,'from',parent_t)
        graph.create(parent_t)
        graph.create(r)

def get_nodes(type):
    res = node_matcher.match(type)
    return list(res)

def get_nodes_by_name(type,name):
    res = node_matcher.match(type, name=name)
    # res = matcher.match('table',name='fscrm.table_a')
    return list(res)

node1 = Node('table',name='table_to_create')
result = relation_matcher.match({node1},'from')

for x in result:
    for y in walk(x):
        if type(y) is Node:
            print (y)

print(get_nodes_by_name('table','fscrm.table_a'))