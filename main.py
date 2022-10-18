import os
import logging

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

DEBUG_MODE = os.getenv('DEBUG','false') == "true"                       # Global DEBUG logging
LOGFORMAT = "%(asctime)s %(funcName)-10s [%(levelname)s] %(message)s"   # Log format


"""
Decocator that times a functions running time
"""
import time
def tajm(func):
    def tajmer(*args, **kwargs):
        start_time = time.perf_counter()
        func(*args, **kwargs)
        logging.info(f"Executed in {time.perf_counter()-start_time:0.2f} seconds.")
    return tajmer


""" 
A callable that runs all neo transactions in a list
"""
@tajm
def _do_transact(tx, trans_list):
    for trans in trans_list:
        tx.run(trans)


""" 
Clears the Neo database
"""
def empty_db(neo):
    logging.info("Clear the Neo database")
    trans_list = ["MATCH (n) DETACH DELETE(n)"]
    neo.execute_write(_do_transact, trans_list)
    

""" 
Create all static objects
"""
def create_static_stuff(neo):
    logging.info("Create all static objects")
    trans_list = [f"CREATE (:Period {{name:{period}}})" for period in range(2017,2024)]
    trans_list.append('CREATE (:`Känsliga personuppgifter` {name:"Känsliga personuppgifter"})')
    neo.execute_write(_do_transact, trans_list)


""" 
Create all Wiki objects
"""
def create_fo_object(neo, wiki):
    logging.info("Create all Wiki objects")
    query = "[[Category:Aktiva_objekt]]|?Objekt|?Status"
    answer = wiki.ask(query)
    trans_list = [f"CREATE (:Objekt {{name:'{object['displaytitle']}', wiki_id:{object['fulltext'].split(':', 1)[1]}, function:'Undefined'}})" for object in answer]
    neo.execute_write(_do_transact, trans_list)


""" 
Create systems and system to FO relationships
"""
def create_systems(neo, wiki):
    logging.info("Create systems and system to FO relationships")
    trans_list = []
    query = "[[Category:Aktiva_system]]|?System|?Funktion|?Tillhörande_objekt"
    answer = wiki.ask(query)

    for system in answer:
        system_name = system["displaytitle"]
        system_id = system["fulltext"].split(":", 1)[1]
        fo_id = system["printouts"]["Tillhörande objekt"][0]["fulltext"].split(":", 1)[1] if system["printouts"]["Tillhörande objekt"] else None
        system_function = system["printouts"]["Funktion"][0] if system["printouts"]["Funktion"] else "Undefined"
        
        trans = f"CREATE (s:System {{name:'{system_name}', wiki_id:{system_id}, function:'{system_function}'}})"
        trans_list.append(trans)
        if fo_id:
            trans = f"MATCH (s:System), (f:Objekt) WHERE s.wiki_id={system_id} and f.wiki_id={fo_id} \
                    CREATE (s)-[r:ingår_i]->(f)"
            trans_list.append(trans)

    neo.execute_write(_do_transact, trans_list)


""" 
Create server and server to system relationships
"""
def create_servers(neo, wiki):
    logging.info("Create server and server to system relationships")
    trans_list = []
    query = "[[Category:Aktiva_moduler]]|?Tillhörande_system"
    answer = wiki.ask(query)

    for server in answer:
        module_name = server["displaytitle"]
        module_url = server["fullurl"]
        module_id = server["fulltext"].split(":", 1)[1]
        if server["printouts"]["Tillhörande system"]:
            temp_id = [system["fulltext"].split(":", 1)[1] for system in server["printouts"]["Tillhörande system"]]
            system_id = ",".join(temp_id)
        else:
            system_id = "Undefined"

        trans = f"CREATE (:Modul {{name:'{module_name}', wiki_id:{module_id}, url:'{module_url}'}})"
        trans_list.append(trans)
        trans = f"MATCH (m:Modul), (s:System) WHERE m.wiki_id={module_id} and s.wiki_id IN [{system_id}] \
                  CREATE (m)-[r:Tillhör]->(s)"
        trans_list.append(trans)

    neo.execute_write(_do_transact, trans_list)


""" 
Create external services
"""
def create_external_services(neo, wiki):
    logging.info("Create external services")
    query = "[[Category:Aktiva_externa_tjänster]]"
    answer = wiki.ask(query)
    trans_list = [f"CREATE (:Extern {{name:'{object['displaytitle']}', wiki_id:{object['fulltext'].split(':', 1)[1]}, url:'{object['fullurl']}'}})" for object in answer]
    neo.execute_write(_do_transact, trans_list)


""" 
Create dependencies
"""
def create_dependencies(neo, wiki):
    logging.info("Create dependencies")
    trans_list = []
    query = "[[Category:Aktiva_beroenden]]|?Från|?Till|?Typ_av_beroende"
    answer = wiki.ask(query)

    for dependency in answer:
        from_obj = dependency["printouts"]["Från"][0]["fulltext"].split(":", 1)
        to_obj = dependency["printouts"]["Till"][0]["fulltext"].split(":", 1)
        dep_type = dependency["printouts"]["Typ av beroende"][0] if dependency["printouts"]["Typ av beroende"] else "Undefined"
        dep_id = dependency["fulltext"].split(":", 1)[1]

        trans = f"MATCH (s1:{from_obj[0]}), (s2:{to_obj[0]}) WHERE s1.wiki_id={from_obj[1]} and s2.wiki_id={to_obj[1]} \
                  CREATE (s1)-[Beroende:`{dep_type}` {{BeroendeUrl: 'https://{ITWIKI_DOMAIN}/wiki/Beroende: {dep_id}', typ:'Systemberoende'}}]->(s2)"
        trans_list.append(trans)

    neo.execute_write(_do_transact, trans_list)


""" 
Create personal data processors
"""
def create_personal_data_processors(neo, wiki):
    logging.info("Create personal data processors")
    trans_list = []
    query = "[[Category:Aktiva_behandlingar]]|?Tillhörande_system|?Behandlas_känsliga_personuppgifter|?Personuppgiftsansvarig|?Ändamål"
    answer = wiki.ask(query)

    for processor in answer:
        processor_name = processor["displaytitle"]
        processor_id = processor["fulltext"].split(":", 1)[1]
        sensitive_data = processor["printouts"]["Behandlas känsliga personuppgifter"][0] if "Behandlas känsliga personuppgifter" in processor["printouts"] else "Undefined"
        purpose = processor["printouts"]["Ändamål"][0] if "Ändamål" in processor["printouts"] else "Undefined"

        if "Tillhörande system" in processor["printouts"]:      # ToDo: Check if correct test
            temp_id = [system["fulltext"].split(":", 1)[1] for system in processor["printouts"]["Tillhörande system"]]
            system_id = ",".join(temp_id)
        else:
            system_id = "Undefined"

        trans = f"CREATE (s:Behandling {{name:'{processor_name}', wiki_id:{processor_id}, Ändamål:'{purpose}', Känsliga_personuppgifter:'{sensitive_data}'}})"
        trans_list.append(trans)
        trans = f"MATCH (b:Behandling), (s:System) WHERE b.wiki_id={processor_id} and s.wiki_id IN [{system_id}] \
                  CREATE (b)-[r:Tillhör]->(s)"
        trans_list.append(trans)
        if sensitive_data == "Ja":
            trans = f"MATCH (b:Behandling), (k:`Känsliga personuppgifter`) WHERE b.wiki_id={processor_id} \
                      CREATE (b)-[:innehåller]->(k)"
            trans_list.append(trans)

    neo.execute_write(_do_transact, trans_list)


""" 
Create object plans
"""
def create_object_plans(neo, wiki):
    logging.info("Create object plans")
    trans_list = []
    query = "[[Category:Planer]]|?Tillhörande_objekt|?Period"
    answer = wiki.ask(query)

    for plan in answer:
        plan_name = plan["displaytitle"]
        plan_period = plan["printouts"]["Period"][0]["raw"].split("/", 1)[1]
        plan_id = plan["fulltext"].split(":", 1)[1]

        if "Tillhörande objekt" in plan["printouts"]:      # ToDo: Check if correct test
            temp_id = [system["fulltext"].split(":", 1)[1] for system in plan["printouts"]["Tillhörande objekt"]]
            fo_id = ",".join(temp_id)
        else:
            fo_id = "Undefined"

        trans = f"MATCH (p:Period) WHERE p.name={plan_period} \
                  CREATE (fp:`Objektplan` {{name:'{plan_name}', wiki_id:{plan_id}, period: {plan_period}}}) \
                  CREATE (fp)-[:aktuell_period]->(p)"
        trans_list.append(trans)
        trans = f"MATCH (fp:Objektplan), (fo:Objekt) WHERE fp.wiki_id={plan_id} and fo.wiki_id={fo_id}\
                  CREATE (fp)-[r:Tillhör {{SystemUrl:'https://{ITWIKI_DOMAIN}/wiki/Plan:{plan_id}', ObjektUrl:'https://{ITWIKI_DOMAIN}/wiki/Objekt:{fo_id}'}}]->(fo)"
        trans_list.append(trans)

    neo.execute_write(_do_transact, trans_list)



""" 
Main function
"""
def main():
    if DEBUG_MODE:       # Debug requested
        logging.basicConfig(level=logging.DEBUG, format=LOGFORMAT)
    logging.basicConfig(level=logging.INFO, format=LOGFORMAT)     # Default logging level

    neo_driver = GraphDatabase.driver(NEO_URI, auth=(NEO_USER, NEO_PWD))
    wiki_session = Site(ITWIKI_DOMAIN, path="/")
    wiki_session.login(ITWIKI_USER, ITWIKI_PWD)

    with neo_driver.session() as neo_session:
        empty_db(neo_session)           # Start by emptying database
        create_static_stuff(neo_session)
        create_fo_object(neo_session, wiki_session)
        create_systems(neo_session, wiki_session)
        create_servers(neo_session, wiki_session)
        create_external_services(neo_session, wiki_session)
        create_dependencies(neo_session, wiki_session)
        create_personal_data_processors(neo_session, wiki_session)
        create_object_plans(neo_session, wiki_session)


""" 
Entry point
"""
if __name__ == '__main__':
    main()          # Just run main
