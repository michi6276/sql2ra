import sqlparse
import radb
from sqlparse.sql import IdentifierList, Identifier
import radb.parse
import radb.ast
from sqlparse.tokens import Keyword, DML
from radb.ast import *
from radb.parse import RAParser as sym
from sqlparse.sql import Where

sql = """select distinct Person.name, pizzeria from Person, Eats, Serves
                       where Person.name = Eats.name and Eats.pizza = Serves.pizza"""
sql2 = "select distinct A.name, B.name from Eats A, Eats B where A.pizza = B.pizza and A.year = B.year"
stmt = sqlparse.parse(sql)[0]
stmt2 = sqlparse.parse(sql2)[0]
tokenlist = stmt.token_next_by(i=sqlparse.sql.TokenList)
identlist = stmt.token_next_by(i=sqlparse.sql.IdentifierList)


def get_Columns(stmt):
    attributes = None
    attr = []
    if "*" not in str(stmt):
        tokenlist = stmt.token_next_by(i=sqlparse.sql.TokenList)
        attributes = str(tokenlist[1])
        attributes = str(stmt[4]).strip() # anders machen
        attributes = attributes.split(",")
        for i in range(len(attributes)):
            att = str(attributes[i]).split(".")
            if len(att) > 1:
                attr.append(AttrRef(att[0],att[1]))
            else:
                attr.append(AttrRef(None, att[0]))
    else:
        attr = None

    return attr

def get_restriction(relation, stmt):
    list = []
    restriction = stmt.token_next_by(i=sqlparse.sql.Where)
    restriction = str(restriction[1])
    if "where" in restriction:
        restriction = restriction.replace("where","").strip()
        res = restriction.split("and ")
        for r in res:
            rest = r.split("=")
            list.append(ValExprBinaryOp(get_AttrRef(rest[0]), sym.EQ, get_AttrRef(rest[1])))
        condition = list[0]
        for i in range(1, len(list)):
            condition = ValExprBinaryOp(condition, sym.AND, list[i])
        return Select(condition,relation)
    else:
        return

def get_Tables(relations):
        joined_relations = relations[0]
        for i in range(1, len(relations)):
            joined_relations = Cross(joined_relations, relations[i])
        return joined_relations


def get_Relations(stmt):
    #tables = stmt.token_next_by(i=sqlparse.sql.IdentifierList)

    stream = get_from_rel(stmt)
    tables = get_rel_ident(stream)
    col_list = []
    for t in tables:
        t = str(t).strip()
        if " " in t:

            relRef = RelRef(t.split(" ")[0])
            rename = Rename(t.split(" ")[1],None, relRef)
            col_list.append(rename)
        else:
            col_list.append(RelRef(str(t)))
            print("hallo")
    return col_list


def get_AttrRef(attribute):
    data = attribute.split('.')
    if len(data) == 1:
        return AttrRef(None, attribute.strip())
    elif len(data) > 1:
        return AttrRef(data[0].strip(), data[1].strip())

def translate(stmt):
    print(stmt)
    rel = get_Relations(stmt)
    tables = get_Tables(rel)
    list = get_Columns(stmt)
    select = get_restriction(tables,stmt)
    if list == None: # Columns
        if  select == None: # Restrictions
            print("1")
            project = tables
        else:
            print("2")
            project = select
    else:
        if select == None:
            print("3")
            project = radb.ast.Project(list, tables)
        else:
            print("4")
            project = radb.ast.Project(list, select)
    relAl = radb.parse.one_statement_from_string(str(project)+";")
    print(str(relAl))
    return relAl

def get_subSelect(token):
    if not token.is_group:
        return False
    for item in token.tokens:
        if item.ttype is DML and item.value.lower() == 'select':
            return True
    return False

def get_from_rel(stmt):
    from_seen = False
    for token in stmt.tokens:
        if from_seen:
            if get_subSelect(token):
                for x in get_from_rel(token):
                    yield x
            elif token.ttype is Keyword:
                raise StopIteration
            else:
                yield token
        elif token.ttype is Keyword and token.value.lower() == 'from':
            from_seen = True

def get_rel_ident(tokens):
    list = []
    for t in tokens:
        if isinstance(t, IdentifierList):
            for identifier in t.get_identifiers():
                list.append(identifier)
        elif isinstance(t, Identifier):
            list.append(t)
        elif t.ttype is Keyword:
            list.append(t.value)
    return list

translate(stmt)