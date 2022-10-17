import os
from neo4j import GraphDatabase
from mwclient import Site


""" 
Environment
"""
NEO_URI = os.getenv("NEO_URI","neo4j://localhost:7687")
NEO_USER = os.getenv("NEO_USER")
NEO_PWD = os.getenv("NEO_PWD")
ITWIKI_DOMAIN = os.getenv("ITWIKI_DOMAIN")
ITWIKI_USER = os.getenv("ITWIKI_USER")
ITWIKI_PWD = os.getenv("ITWIKI_PWD")


""" 
A callable that runs all neo transactions in a list
"""
def _do_transact(tx, trans_list):
    for trans in trans_list:
        tx.run(trans)


""" 
Clears the Neo database
"""
def empty_db(neo):
    trans_list = ["MATCH (n) DETACH DELETE(n)"]
    neo.execute_write(_do_transact, trans_list)
    

""" 
Create all static objects
"""
def create_static_stuff(neo):
    trans_list = [f"CREATE (:Period {{name:{period}}})" for period in range(2017,2024)]
    trans_list.append('CREATE (:`Känsliga personuppgifter` {name:"Känsliga personuppgifter"})')
    neo.execute_write(_do_transact, trans_list)


""" 
Create all Wiki objects
"""
def create_fo_object(neo, wiki):
    query = "[[Category:Aktiva_objekt]]|?Objekt|?Status"
    answers = wiki.ask(query)
    trans_list = [f"CREATE (s:Objekt {{name:'{answer['displaytitle']}', wiki_id:'{answer['fulltext'].split(':', 1)[1]}', function:'Undefined'}})" for answer in answers]
    neo.execute_write(_do_transact, trans_list)


""" 
Create systems and system to FO relationships
"""
def create_systems(neo, wiki):
    query = "[[Category:Aktiva_system]]|?System|?Funktion|?Tillhörande_objekt"
    trans_list = []
    answers = wiki.ask(query)
    for answer in answers:
        system_name = answer["displaytitle"]
        system_id = answer["fulltext"].split(":", 1)[1]
        fo_id = answer["printouts"]["Tillhörande objekt"][0]["fulltext"].split(":", 1)[1]
        system_function = answer["printouts"]["Funktion"][0] if "Funktion" in answer["printouts"]["Funktion"] else "Undefined"
        
        trans = f"CREATE (s:System {{name:'{system_name}', wiki_id:'{system_id}', function:'{system_function}'}})"
        trans_list.append(trans)
        trans = f"MATCH (s:System), (f:Objekt) WHERE s.wiki_id='{system_id}' and f.wiki_id='{fo_id}'\n CREATE(s)-[r:ingår_i]->(f)"
        trans_list.append(trans)

    neo.execute_write(_do_transact, trans_list)


def main():
    neo_driver = GraphDatabase.driver(NEO_URI, auth=(NEO_USER, NEO_PWD))
    wiki_session = Site(ITWIKI_DOMAIN, path="/")
    wiki_session.login(ITWIKI_USER, ITWIKI_PWD)

    with neo_driver.session() as neo_session:
        empty_db(neo_session)           # Start by emptying database
        create_static_stuff(neo_session)
        create_fo_object(neo_session, wiki_session)
        create_systems(neo_session, wiki_session)


if __name__ == '__main__':
    main()