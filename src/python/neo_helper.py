from py2neo import Node, Relationship,Graph,NodeMatcher,RelationshipMatcher,walk


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

def exe_cypher(cypher):
    graph.run(cypher)

def add_node(node):
    graph.create(node)

def delte_node_by_name(label,name):
    table = get_nodes_by_name(label,name)
    graph.delete(table.pop())

def update_node_name(name,new_name):
    update_cypher = "MATCH (n { name: $name }) SET n.name = $new_name RETURN n.name"
    return graph.run(update_cypher,name=name,new_name=new_name).data()

def get_nodes(type):
    res = node_matcher.match(type)
    return list(res)

def get_nodes_by_name(type,name):
    res = node_matcher.match(type, name=name)
    # res = matcher.match('table',name='fscrm.table_a')
    return list(res)

print(update_node_name('hello.hello','hello.hello1'))


def query_relation(relation):
    result = relation_matcher.match(get_nodes_by_name('table','table_c'),'from')
    for x in result:
        for y in walk(x):
            if type(y) is Node:
                print (y)






